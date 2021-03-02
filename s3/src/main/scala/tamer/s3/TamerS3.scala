package tamer.s3

import com.sksamuel.avro4s.Codec
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.TamerError
import tamer.job.{AbstractTamerJob, SourceStateChanged}
import tamer.kafka.Kafka
import zio.ZIO.when
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.s3.{ListObjectOptions, S3}
import zio.stream.ZStream
import zio.{Queue, Schedule, Task, ZIO}

import java.time.Duration
import scala.math.Ordering.Implicits.infixOrderingOps

trait TamerS3 {
  def fetch[
    K <: Product : Codec,
    V <: Product : Codec,
    S <: Product : Codec
  ](
     setup: S3Configuration[K, V, S]
   ): ZIO[zio.s3.S3 with Kafka with Blocking with Clock, TamerError, Unit]
}

object TamerS3 {

  class TamerS3Job[
    K <: Product : Codec,
    V <: Product : Codec,
    S <: Product : Codec
  ](
     setup: S3Configuration[K, V, S]
   ) extends AbstractTamerJob[K, V, S, Keys, zio.s3.S3 with Kafka with Blocking with Clock](setup.generic) {
    private final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.s3")


    override protected def createInitialSourceState(): Keys = List.empty[String]

    override protected def cappedExponentialBackoff: Schedule[Any, Any, (Duration, Long)] = Schedule.exponential(
      setup.pollingTimings.minimumIntervalForBucketFetch
    ) || Schedule.spaced(
      setup.pollingTimings.maximumIntervalForBucketFetch
    )

    override protected def updateSourceState(
                                              currentSourceState: SourceStateRef,
                                              sourceChangedToken: Queue[Unit]
                                           ): ZIO[zio.s3.S3 with Kafka with Blocking with Clock, Throwable, SourceStateChanged] = {
      val paginationMaxKeys = 1000L
      val paginationMaxPages = 1000L
      val defaultTimeoutBucketFetch = 60.seconds
      val timeoutForFetchAllKeys: Duration =
        if (setup.pollingTimings.minimumIntervalForBucketFetch < defaultTimeoutBucketFetch) {
          setup.pollingTimings.minimumIntervalForBucketFetch
        } else {
          defaultTimeoutBucketFetch
        }

      def warnAboutSpuriousKeys(log: LogWriter[Task], keyList: List[String]) = {
        val witness = keyList.find(key => !key.startsWith(setup.prefix)).getOrElse("")
        log.warn(s"""Server returned '$witness' (and maybe more files) which don't match prefix "${setup.prefix}"""")
      }

      for {
        log <- logTask
        _ <- log.info(s"getting list of keys in bucket ${setup.bucketName} with prefix ${setup.prefix}")
        initialObjListing <- zio.s3.listObjects(setup.bucketName, ListObjectOptions.from(setup.prefix, paginationMaxKeys))
        allObjListings <- zio.s3
          .paginate(initialObjListing)
          .take(paginationMaxPages)
          .timeout(timeoutForFetchAllKeys)
          .runCollect
          .map(_.toList)
        keyList = allObjListings
          .flatMap(objListing => objListing.objectSummaries)
          .map(_.key)
        cleanKeyList = keyList.filter(_.startsWith(setup.prefix))
        _ <- when(keyList.size != cleanKeyList.size)(warnAboutSpuriousKeys(log, keyList))
        _ <- log.debug(s"Current key list has ${cleanKeyList.length} elements")
        _ <- log.debug(s"The first and last elements are ${cleanKeyList.sorted.headOption} and ${cleanKeyList.sorted.lastOption}")
        previousListOfKeys <- currentSourceState.getAndSet(cleanKeyList)
        detectedKeyListChanged = cleanKeyList.sorted != previousListOfKeys.sorted
        _ <- when(detectedKeyListChanged)(log.debug("Detected change in key list") *> sourceChangedToken.offer(()))
      } yield if (detectedKeyListChanged) SourceStateChanged(true) else SourceStateChanged(false)
    }

    protected final def iteration(
                                   currentSourceState: SourceStateRef,
                                   sourceChangedToken: Queue[Unit]
                                 )(
                                   currentState: S,
                                   q: Queue[(K, V)]
                                 ): ZIO[zio.s3.S3 with Kafka with Blocking with Clock, TamerError, S] =
      (for {
        log <- logTask
        nextState <- setup.transitions.getNextState(currentSourceState, currentState, sourceChangedToken)
        _ <- log.debug(s"Next state computed to be $nextState")
        keys <- currentSourceState.get
        optKey = setup.transitions.selectObjectForState(nextState, keys)
        _ <- log.debug(s"Will ask for key $optKey")
        key <- ZIO.getOrFailWith(TamerError(s"File not found with key $optKey for state $nextState"))(optKey)
        _ <- {
          val transduced: ZStream[S3, RuntimeException, V] = zio.s3
            .getObject(setup.bucketName, key)
            .transduce(setup.transducer)
          transduced.foreach(value => q.offer(setup.transitions.deriveKafkaRecordKey(nextState, value) -> value))
        } // FIXME: relies on nextState.toString
      } yield nextState).mapError(e => TamerError("Error while doing iterationTimeBased", e))
  }


  class TamerS3Impl extends TamerS3 {

    final def fetch[
      K <: Product : Codec,
      V <: Product : Codec,
      S <: Product : Codec
    ](
       setup: S3Configuration[K, V, S]
     ): ZIO[zio.s3.S3 with Kafka with Blocking with Clock, TamerError, Unit] = new TamerS3Job[K, V, S](setup).fetch()


  }

}