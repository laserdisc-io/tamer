package tamer
package s3

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.config.Config
import tamer.kafka.Kafka
import zio.ZIO.{effectSuspendTotal, unit}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.stream.{Transducer, ZTransducer}
import zio.{Queue, Task, ZIO, _}

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneId}

final case class S3Object(bucketName: String, key: String)
final case class LastProcessedInstant(instant: Instant) // TODO: can't be AnyVal because we want to encode a record

final case class ZonedDateTimeFormatter(dateTimeFormatter: DateTimeFormatter, zone: ZoneId)
final case class Line(str: String) // TODO: can't be AnyVal because of tamer.Serde

object S3 {
  private final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.s3")

  def whenSome[R, E, A](b: =>Option[A])(zio: A => ZIO[R, E, Any]): ZIO[R, E, Unit] =
    effectSuspendTotal(if (b.isDefined) zio(b.get).unit else unit) // TODO: unit-tests

  private final val kafkaLayer: Layer[TamerError, Kafka] = Config.live >>> Kafka.live

  type KeysR = Ref[List[String]]
  val createRefToListOfKeys: UIO[KeysR] = Ref.make(List.empty[String])

  private val defaultTransducer: Transducer[Nothing, Byte, Line] = ZTransducer.utf8Decode >>> ZTransducer.splitLines.map(Line)

  def updateListOfKeys(keysR: KeysR, bucketName: String, prefix: String): ZIO[zio.s3.S3 with Clock, Throwable, Unit] =
    for {
      log               <- logTask
      _                 <- log.debug("updating list of filenames...")
      _                 <- log.debug(s"will look inside bucket $bucketName, with prefix $prefix")
      initialObjListing <- zio.s3.listObjects(bucketName, prefix, 1000)
      allObjListings <- zio.s3
        .paginate(initialObjListing)
        .take(1000)          // TODO: put sensible value
        .timeout(60.seconds) // TODO: put sensible value (constrained by setup.minimumIntervalForBucketFetch)
        .runCollect
        .map(_.toList.appended(initialObjListing))
      listOfFilenames = allObjListings
        .flatMap(objListing => objListing.objectSummaries)
        .map(_.key)
      _ <- keysR.set(listOfFilenames)
    } yield ()

  final def fetch[V <: Product: Encoder: Decoder: SchemaFor](
      bucketName: String,
      prefix: String,
      afterwards: LastProcessedInstant,
      transducer: Transducer[TamerError, Byte, V] = defaultTransducer,
      parallelism: PosInt = 1,
      dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT, // TODO: type this something more meaningful
      minimumIntervalForBucketFetch: Duration = 5.minutes
  ): ZIO[Blocking with Clock with zio.s3.S3, TamerError, Unit] = {
    val setup =
      Setup.fromDateTimeFormatter[V](bucketName, prefix, afterwards, transducer, parallelism, dateTimeFormatter, minimumIntervalForBucketFetch)
    (for {
      keysR <- createRefToListOfKeys
      _ <- updateListOfKeys(keysR, setup.bucketName, setup.prefix)
        .repeat(Schedule.spaced(setup.minimumIntervalForBucketFetch)) // TODO: use exponential backup instead of fixed value
        .fork
      _ <- tamer.kafka.runLoop(setup)(tamer.s3.S3.iteration(setup, keysR))
    } yield ()).provideSomeLayer[Blocking with Clock with zio.s3.S3](kafkaLayer)
  }

  def suffixWithoutFileExtension(key: String, prefix: String): String = {
    val keyWithoutExtension = if (key.contains('.')) key.substring(0, key.indexOf('.')) else key // TODO: what about a dot in the date?
    keyWithoutExtension.stripPrefix(prefix)
  }
  def parseInstantFromKey(key: String, prefix: String, dateTimeFormatter: DateTimeFormatter): Instant =
    Instant.from(dateTimeFormatter.parse(suffixWithoutFileExtension(key, prefix)))
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

  final def iteration[V <: Product: Encoder: Decoder: SchemaFor](
      setup: Setup[V],
      keysR: KeysR
  )(afterwards: LastProcessedInstant, q: Queue[(S3Object, V)]): ZIO[zio.s3.S3, TamerError, LastProcessedInstant] =
    (for {
      log              <- logTask
      nextFileDateTime <- getNextInstant(keysR, afterwards, setup.prefix, setup.dateTimeFormatter)
      nextState = LastProcessedInstant(nextFileDateTime.getOrElse(afterwards.instant))
      _    <- log.debug(s"Next state computed to be $nextState")
      keys <- keysR.get
      _    <- log.debug(s"Current keys cache has ${keys.length} elements")
      _    <- log.debug(s"The first and last elements are ${keys.headOption} and ${keys.lastOption}")
      _ <- whenSome(nextFileDateTime) { instant =>
        val optKey = deriveKey(instant, setup.dateTimeFormatter, keys)
        log.debug(s"will ask for key $optKey") *> optKey
          .map(key =>
            zio.s3
              .getObject(setup.bucketName, key)
              .transduce(setup.transducer)
              .foreach(value => q.offer(S3Object(setup.bucketName, key) -> value))
          )
          .getOrElse(ZIO.fail(TamerError(s"File not found with date $instant")))
      }
      // TODO: choose encoding
    } yield nextState).mapError(e => TamerError("Error while doing iteration", e))
}
