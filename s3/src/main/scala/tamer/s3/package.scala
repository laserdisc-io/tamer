package tamer

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.config.{Config, KafkaConfig}
import tamer.kafka.Kafka
import zio.ZIO.when
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.s3.{ListObjectOptions, S3}
import zio.stream.{Transducer, ZTransducer}
import zio.{Queue, Task, ZIO, _}

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneId}
import scala.math.Ordering.Implicits.infixOrderingOps

case class KeysChanged(differenceFound: Boolean) extends AnyVal

package object s3 {
  private final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.s3")

  private final def kafkaLayer(kafkaConfigLayer: Layer[TamerError, KafkaConfig]): Layer[TamerError, Kafka] = kafkaConfigLayer >>> Kafka.live

  type KeysR = Ref[List[String]]
  type Keys  = List[String]
  val createRefToListOfKeys: UIO[KeysR] = Ref.make(List.empty[String])

  private val defaultTransducer: Transducer[Nothing, Byte, Line] =
    ZTransducer.utf8Decode >>> ZTransducer.splitLines.map(Line)

  def updateListOfKeys(
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

  final def fetchAccordingToSuffixDate[R, K <: Product: Encoder: Decoder: SchemaFor, V <: Product: Encoder: Decoder: SchemaFor](
      bucketName: String,
      prefix: String,
      afterwards: LastProcessedInstant,
      deriveKafkaKey: (LastProcessedInstant, V) => K = (l: LastProcessedInstant, _: V) => l,
      transducer: ZTransducer[R, TamerError, Byte, V] = defaultTransducer,
      parallelism: PosInt = 1,
      dateTimeFormatter: ZonedDateTimeFormatter = ZonedDateTimeFormatter(DateTimeFormatter.ISO_INSTANT, ZoneId.systemDefault()),
      minimumIntervalForBucketFetch: Duration = 5.minutes,
      maximumIntervalForBucketFetch: Duration = 5.minutes,
      kafkaConfig: Task[Config.Kafka] = ZIO.service[Config.Kafka].provideLayer(Config.live)
  ): ZIO[R with Blocking with Clock with zio.s3.S3, TamerError, Unit] = {
    val setup =
      Setup.mkTimeBased[R, K, V](
        bucketName,
        prefix,
        afterwards,
        transducer,
        parallelism,
        dateTimeFormatter,
        minimumIntervalForBucketFetch,
        maximumIntervalForBucketFetch,
        deriveKafkaKey
      )
    fetch(setup).provideSomeLayer[R with Blocking with Clock with zio.s3.S3](
      kafkaLayer(kafkaConfig.toLayer.mapError(e => TamerError("Error while fetching default kafka configuration", e)))
    )
  }

  final def fetch[
      R,
      K <: Product: Encoder: Decoder: SchemaFor,
      V <: Product: Encoder: Decoder: SchemaFor,
      S <: Product: Encoder: Decoder: SchemaFor
  ](
      setup: Setup[R, K, V, S]
  ): ZIO[R with zio.s3.S3 with Kafka with Blocking with Clock, TamerError, Unit] =
    for {
      keysR <- createRefToListOfKeys
      cappedExponentialBackoff: Schedule[Any, Any, (Duration, Long)] = Schedule.exponential(setup.minimumIntervalForBucketFetch) || Schedule.spaced(
        setup.maximumIntervalForBucketFetch
      )

      keysChangedToken <- Queue.dropping[Unit](requestedCapacity = 1)
      updateListOfKeysM = updateListOfKeys(keysR, setup.bucketName, setup.prefix, setup.minimumIntervalForBucketFetch, keysChangedToken)
      updateKeysFiber <- updateListOfKeysM
        .scheduleFrom(KeysChanged(true))(
          Schedule.once andThen cappedExponentialBackoff.untilInput(_ == KeysChanged(true))
        )
        .forever
        .fork
      _ <- tamer.kafka.runLoop(setup)(iteration(setup, keysR, keysChangedToken)).ensuring(updateKeysFiber.interrupt)
    } yield ()

  private final def iteration[
      R,
      K <: Product: Encoder: Decoder: SchemaFor,
      V <: Product: Encoder: Decoder: SchemaFor,
      S <: Product: Encoder: Decoder: SchemaFor
  ](
      setup: Setup[R, K, V, S],
      keysR: KeysR,
      keysChangedToken: Queue[Unit]
  )(
      currentState: S,
      q: Queue[Chunk[(K, V)]]
  ): ZIO[R with zio.s3.S3, TamerError, S] =
    (for {
      log       <- logTask
      nextState <- setup.getNextState(keysR, currentState, keysChangedToken)
      _         <- log.debug(s"Next state computed to be $nextState")
      keys      <- keysR.get
      optKey = setup.selectObjectForState(nextState, keys)
      _ <- log.debug(s"Will ask for key $optKey") *> optKey
        .map(key =>
          zio.s3
            .getObject(setup.bucketName, key)
            .transduce(setup.transducer)
            .map(value => (setup.deriveKafkaRecordKey(nextState, value), value))
            .foreachChunk(q.offer)
        )
        .getOrElse(ZIO.fail(TamerError(s"File not found with key $optKey for state $nextState"))) // FIXME: relies on nextState.toString
    } yield nextState).mapError(e => TamerError("Error while doing iterationTimeBased", e))
}
