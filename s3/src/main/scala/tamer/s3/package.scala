package tamer

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.config.Config
import tamer.kafka.Kafka
import zio.ZIO.{effectSuspendTotal, unit}
import zio.blocking.{Blocking, blocking}
import zio.clock.Clock
import zio.duration.durationInt
import zio.s3.S3
import zio.stream.{Transducer, ZTransducer}
import zio.{Queue, Task, ZIO, _}

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneId}
import scala.math.Ordering.Implicits.infixOrderingOps

case class KeysChanged(differenceFound: Boolean) extends AnyVal

package object s3 {

  /** executes the provided ZIO if the passed option contains a value, passing the content to the ZIO itself
    */
  def whenSome[R, E, A](a: =>Option[A])(zio: A => ZIO[R, E, Any]): ZIO[R, E, Unit] =
    effectSuspendTotal(if (a.isDefined) zio(a.get).unit else unit)

  private final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.s3")

  private final val kafkaLayer: Layer[TamerError, Kafka] = Config.live >>> Kafka.live

  type KeysR = Ref[List[String]]
  val createRefToListOfKeys: UIO[KeysR] = Ref.make(List.empty[String])

  private val defaultTransducer: Transducer[Nothing, Byte, Line] = ZTransducer.utf8Decode >>> ZTransducer.splitLines.map(Line)

  def updateListOfKeys(
      keysR: KeysR,
      bucketName: String,
      prefix: String,
      minimumIntervalForBucketFetch: Duration
  ): ZIO[S3 with Clock, Throwable, KeysChanged] = {
    val paginationMaxKeys         = 1000L
    val paginationMaxPages        = 1000L
    val defaultTimeoutBucketFetch = 60.seconds
    val timeoutForFetchAllKeys: Duration =
      if (minimumIntervalForBucketFetch < defaultTimeoutBucketFetch) minimumIntervalForBucketFetch else defaultTimeoutBucketFetch

    for {
      log               <- logTask
      _                 <- log.info(s"getting list of keys in bucket $bucketName with prefix $prefix")
      initialObjListing <- zio.s3.listObjects(bucketName, prefix, paginationMaxKeys)
      allObjListings <- zio.s3
        .paginate(initialObjListing)
        .take(paginationMaxPages)
        .timeout(timeoutForFetchAllKeys)
        .runCollect
        .map(_.toList :+ initialObjListing)
      keyList = allObjListings
        .flatMap(objListing => objListing.objectSummaries)
        .map(_.key)
      _                  <- log.debug(s"Current key list has ${keyList.length} elements")
      _                  <- log.debug(s"The first and last elements are ${keyList.sorted.headOption} and ${keyList.sorted.lastOption}")
      previousListOfKeys <- keysR.getAndSet(keyList)
    } yield if (keyList.sorted == previousListOfKeys.sorted) KeysChanged(false) else KeysChanged(true)
  }

  final def fetch[V <: Product: Encoder: Decoder: SchemaFor](
      bucketName: String,
      prefix: String,
      afterwards: LastProcessedInstant,
      transducer: Transducer[TamerError, Byte, V] = defaultTransducer,
      parallelism: PosInt = 1,
      dateTimeFormatter: ZonedDateTimeFormatter = ZonedDateTimeFormatter(DateTimeFormatter.ISO_INSTANT, ZoneId.systemDefault()),
      minimumIntervalForBucketFetch: Duration = 5.minutes,
      maximumIntervalForBucketFetch: Duration = 5.minutes
  ): ZIO[Blocking with Clock with zio.s3.S3, TamerError, Unit] = {
    val setup =
      Setup.fromZonedDateTimeFormatter[V](bucketName, prefix, afterwards, transducer, parallelism, dateTimeFormatter, minimumIntervalForBucketFetch)
    (for {
      keysR <- createRefToListOfKeys
      cappedExponentialBackoff = Schedule.exponential(minimumIntervalForBucketFetch) || Schedule.spaced(maximumIntervalForBucketFetch)

      updateListOfKeysM = updateListOfKeys(keysR, setup.bucketName, setup.prefix, minimumIntervalForBucketFetch)
      _ <- updateListOfKeysM
        .scheduleFrom(KeysChanged(true))(Schedule.once andThen cappedExponentialBackoff.untilInput(_ == KeysChanged(true)))
        .forever
        .fork
      _ <- tamer.kafka.runLoop(setup)(dedupingIterationBlocking(setup, keysR))
    } yield ()).provideSomeLayer[Blocking with Clock with zio.s3.S3](kafkaLayer)
  }

  def suffixWithoutFileExtension(key: String, prefix: String, dateTimeFormatter: DateTimeFormatter): String = {
    val dotCountInDate      = dateTimeFormatter.format(Instant.EPOCH).count(_ == '.')
    val keyWithoutExtension = if (key.count(_ == '.') > dotCountInDate) key.split('.').splitAt(dotCountInDate + 1)._1.mkString(".") else key
    keyWithoutExtension.stripPrefix(prefix)
  }
  def parseInstantFromKey(key: String, prefix: String, dateTimeFormatter: DateTimeFormatter): Instant =
    Instant.from(dateTimeFormatter.parse(suffixWithoutFileExtension(key, prefix, dateTimeFormatter)))
  def deriveKey(instant: Instant, dateTimeFormatter: DateTimeFormatter, keys: List[String]): Option[String] =
    keys.find(_.contains(dateTimeFormatter.format(instant)))
  def getNextInstant(
      keysR: KeysR,
      afterwards: LastProcessedInstant,
      prefix: String,
      dateTimeFormatter: DateTimeFormatter
  ): ZIO[Any, Nothing, Option[Instant]] = keysR.get.map { keys =>
    val sortedFileDates = keys
      .map(key => parseInstantFromKey(key, prefix, dateTimeFormatter))
      .filter(_.isAfter(afterwards.instant))
      .sorted

    sortedFileDates.headOption
  }

  private final def iteration[V <: Product: Encoder: Decoder: SchemaFor](
      setup: Setup[V],
      keysR: KeysR
  )(afterwards: LastProcessedInstant, q: Queue[(tamer.s3.S3Object, V)]): ZIO[zio.s3.S3, TamerError, LastProcessedInstant] =
    (for {
      log         <- logTask
      nextInstant <- getNextInstant(keysR, afterwards, setup.prefix, setup.zonedDateTimeFormatter.value)
      nextState = LastProcessedInstant(nextInstant.getOrElse(afterwards.instant))
      _    <- log.debug(s"Next state computed to be $nextState")
      keys <- keysR.get
      _ <- whenSome(nextInstant) { instant =>
        val optKey = deriveKey(instant, setup.zonedDateTimeFormatter.value, keys)
        log.debug(s"Will ask for key $optKey") *> optKey
          .map(key =>
            zio.s3
              .getObject(setup.bucketName, key)
              .transduce(setup.transducer)
              .foreach(value => q.offer(tamer.s3.S3Object(setup.bucketName, key) -> value))
          )
          .getOrElse(ZIO.fail(TamerError(s"File not found with date $instant")))
      }
    } yield nextState).mapError(e => TamerError("Error while doing iteration", e))

  private final def dedupingIteration[V <: Product: Encoder: Decoder: SchemaFor](
      setup: Setup[V],
      keysR: KeysR
  )(currentState: LastProcessedInstant, q: Queue[(tamer.s3.S3Object, V)]): ZIO[zio.s3.S3 with Clock, TamerError, LastProcessedInstant] =
    (iteration(setup, keysR)(currentState, q) <&> logTask)
      .flatMap { case (nextState, log) =>
        if (nextState == currentState)
          log.info(s"State is still $currentState, waiting ${setup.minimumIntervalForBucketFetch}") *>
            clock.sleep(setup.minimumIntervalForBucketFetch) *>
            dedupingIteration(setup, keysR)(currentState, q)
        else UIO(nextState)
      }
      .mapError(t => TamerError("Error while wrapping iteration", t))

  private final def dedupingIterationBlocking[V <: Product: Encoder: Decoder: SchemaFor](
      setup: Setup[V],
      keysR: KeysR
  )(currentState: LastProcessedInstant, q: Queue[(tamer.s3.S3Object, V)]) =
    blocking(dedupingIteration(setup, keysR)(currentState, q))
  // FIXME: should spawn thread only if necessary but since for S3 this operation is likely time consuming this is acceptable as workaround
}
