package tamer.s3

import com.sksamuel.avro4s.Codec
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.TamerError
import tamer.config.KafkaConfig
import tamer.kafka.Kafka
import zio.ZIO.when
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.s3.{ListObjectOptions, S3}
import zio.{Chunk, Queue, Ref, Schedule, Task, UIO, ZIO}

import java.time.Duration
import scala.math.Ordering.Implicits.infixOrderingOps

trait TamerS3 {
  def fetch[
      R,
      K <: Product: Codec,
      V <: Product: Codec,
      S <: Product: Codec
  ](
      setup: S3Configuration[R, K, V, S]
  ): ZIO[R with zio.s3.S3 with Blocking with Clock with KafkaConfig, TamerError, Unit]
}

object TamerS3 {
  class TamerS3Impl extends TamerS3 {

    private val createRefToListOfKeys: UIO[KeysR]    = Ref.make(List.empty[String])
    private final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.s3")

    final def fetch[
        R,
        K <: Product: Codec,
        V <: Product: Codec,
        S <: Product: Codec
    ](
        setup: S3Configuration[R, K, V, S]
    ): ZIO[R with S3 with Blocking with Clock with KafkaConfig, TamerError, Unit] = for {
      keysR <- createRefToListOfKeys
      cappedExponentialBackoff: Schedule[Any, Any, (Duration, Long)] = Schedule.exponential(
        setup.pollingTimings.minimumIntervalForBucketFetch
      ) || Schedule.spaced(
        setup.pollingTimings.maximumIntervalForBucketFetch
      )

      keysChangedToken <- Queue.dropping[Unit](requestedCapacity = 1)
      updateListOfKeysM = updateListOfKeys(
        keysR,
        setup.bucketName,
        setup.prefix,
        setup.pollingTimings.minimumIntervalForBucketFetch,
        keysChangedToken
      )
      _ <- updateListOfKeysM
        .scheduleFrom(KeysChanged(true))(
          Schedule.once andThen cappedExponentialBackoff.untilInput(_ == KeysChanged(true))
        )
        .forever
        .fork
      kafkaLayer = Kafka.live(setup.generic, iteration(setup, keysR, keysChangedToken))
      _ <- tamer.kafka.runLoop.provideSomeLayer[R with S3 with Blocking with Clock with KafkaConfig](kafkaLayer)
    } yield ()

    private def updateListOfKeys(
        keysR: KeysR,
        bucketName: String,
        prefix: String,
        minimumIntervalForBucketFetch: Duration,
        keysChangedToken: Queue[Unit]
    ): ZIO[S3 with Clock, Throwable, KeysChanged] = {
      val paginationMaxKeys         = 1000L
      val paginationMaxPages        = 1000L
      val defaultTimeoutBucketFetch = 60.seconds
      val timeoutForFetchAllKeys: Duration =
        if (minimumIntervalForBucketFetch < defaultTimeoutBucketFetch) minimumIntervalForBucketFetch
        else defaultTimeoutBucketFetch

      def warnAboutSpuriousKeys(log: LogWriter[Task], keyList: List[String]) = {
        val witness = keyList.find(key => !key.startsWith(prefix)).getOrElse("")
        log.warn(s"""Server returned '$witness' (and maybe more files) which don't match prefix "$prefix"""")
      }

      for {
        log               <- logTask
        _                 <- log.info(s"getting list of keys in bucket $bucketName with prefix $prefix")
        initialObjListing <- zio.s3.listObjects(bucketName, ListObjectOptions.from(prefix, paginationMaxKeys))
        allObjListings <- zio.s3
          .paginate(initialObjListing)
          .take(paginationMaxPages)
          .timeout(timeoutForFetchAllKeys)
          .runCollect
          .map(_.toList)
        keyList = allObjListings
          .flatMap(objListing => objListing.objectSummaries)
          .map(_.key)
        cleanKeyList = keyList.filter(_.startsWith(prefix))
        _                  <- when(keyList.size != cleanKeyList.size)(warnAboutSpuriousKeys(log, keyList))
        _                  <- log.debug(s"Current key list has ${cleanKeyList.length} elements")
        _                  <- log.debug(s"The first and last elements are ${cleanKeyList.sorted.headOption} and ${cleanKeyList.sorted.lastOption}")
        previousListOfKeys <- keysR.getAndSet(cleanKeyList)
        detectedKeyListChanged = cleanKeyList.sorted != previousListOfKeys.sorted
        _ <- when(detectedKeyListChanged)(log.debug("Detected change in key list") *> keysChangedToken.offer(()))
      } yield if (detectedKeyListChanged) KeysChanged(true) else KeysChanged(false)
    }

    private final def iteration[
        R,
        K <: Product: Codec,
        V <: Product: Codec,
        S <: Product: Codec
    ](
        setup: S3Configuration[R, K, V, S],
        keysR: KeysR,
        keysChangedToken: Queue[Unit]
    )(
        currentState: S,
        q: Queue[Chunk[(K, V)]]
    ): ZIO[R with zio.s3.S3, TamerError, S] =
      (for {
        log       <- logTask
        nextState <- setup.transitions.getNextState(keysR, currentState, keysChangedToken)
        _         <- log.debug(s"Next state computed to be $nextState")
        keys      <- keysR.get
        optKey = setup.transitions.selectObjectForState(nextState, keys)
        _ <- log.debug(s"Will ask for key $optKey") *> optKey
          .map(key =>
            zio.s3
              .getObject(setup.bucketName, key)
              .transduce(setup.transducer)
              .map(value => (setup.transitions.deriveKafkaRecordKey(nextState, value), value))
              .foreachChunk(q.offer)
          )
          .getOrElse(ZIO.fail(TamerError(s"File not found with key $optKey for state $nextState"))) // FIXME: relies on nextState.toString
      } yield nextState).mapError(e => TamerError("Error while doing iterationTimeBased", e))
  }

}
