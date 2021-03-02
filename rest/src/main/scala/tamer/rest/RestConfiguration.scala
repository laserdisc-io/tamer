package tamer.rest

import com.sksamuel.avro4s.{Codec, SchemaFor}
import eu.timepit.refined.types.numeric.PosInt
import tamer.{SourceConfiguration, TamerError}
import tamer.job.ZonedDateTimeFormatter
import zio.stream.ZTransducer
import zio.{Queue, UIO, ZIO}

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant}
import scala.util.hashing.MurmurHash3.stringHash

final case class RestConfiguration[
    K <: Product: Codec: SchemaFor,
    V <: Product: Codec: SchemaFor,
    S <: Product: Codec: SchemaFor
](
   bucketName: String,
   prefix: String,
   tamerStateKafkaRecordKey: Int,
   transducer: ZTransducer[Any, TamerError, Byte, V],
   parallelism: PosInt,
   pollingTimings: RestConfiguration.S3PollingTimings,
   transitions: RestConfiguration.State[K, V, S]
) {
  val generic: SourceConfiguration[K, V, S] = SourceConfiguration[K, V, S](
    SourceConfiguration.SourceSerde[K, V, S](),
    defaultState = transitions.initialState,
    tamerStateKafkaRecordKey = tamerStateKafkaRecordKey,
    RestConfiguration.this.toString
  )
}

object RestConfiguration {

  case class S3PollingTimings(
      minimumIntervalForBucketFetch: Duration,
      maximumIntervalForBucketFetch: Duration
  )

  case class State[K, V, S](
      initialState: S,
      getNextState: (KeysR, S, Queue[Unit]) => UIO[S],
      deriveKafkaRecordKey: (S, V) => K,
      selectObjectForState: (S, Keys) => Option[String]
  )

  private[rest] final def suffixWithoutFileExtension(key: String, prefix: String, dateTimeFormatter: DateTimeFormatter): String = {
    val dotCountInDate = dateTimeFormatter.format(Instant.EPOCH).count(_ == '.')
    val keyWithoutExtension =
      if (key.count(_ == '.') > dotCountInDate) key.split('.').splitAt(dotCountInDate + 1)._1.mkString(".") else key
    keyWithoutExtension.stripPrefix(prefix)
  }

  private[rest] final def parseInstantFromKey(key: String, prefix: String, dateTimeFormatter: DateTimeFormatter): Instant =
    Instant.from(dateTimeFormatter.parse(suffixWithoutFileExtension(key, prefix, dateTimeFormatter)))

  private final def getNextInstant(
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

  private[rest] final def getNextState(prefix: String, dateTimeFormatter: DateTimeFormatter)(
      keysR: KeysR,
      afterwards: LastProcessedInstant,
      keysChangedToken: Queue[Unit]
  ): UIO[LastProcessedInstant] = {
    val retryAfterWaitingForKeyListChange =
      keysChangedToken.take *> getNextState(prefix, dateTimeFormatter)(keysR, afterwards, keysChangedToken)
    getNextInstant(keysR, afterwards, prefix, dateTimeFormatter)
      .flatMap {
        case Some(newInstant) if newInstant.isAfter(afterwards.instant) => UIO(LastProcessedInstant(newInstant))
        case _                                                          => retryAfterWaitingForKeyListChange
      }
  }

  private final def selectObjectForInstant(
      zonedDateTimeFormatter: ZonedDateTimeFormatter
  )(lastProcessedInstant: LastProcessedInstant, keys: Keys): Option[String] =
    keys.find(_.contains(zonedDateTimeFormatter.value.format(lastProcessedInstant.instant)))

  final def mkTimeBased[K <: Product: Codec: SchemaFor, V <: Product: Codec: SchemaFor](
      bucketName: String,
      filePathPrefix: String,
      afterwards: LastProcessedInstant,
      context: TamerS3SuffixDateFetcher.Context[K, V]
  ): RestConfiguration[K, V, LastProcessedInstant] =
    RestConfiguration[K, V, LastProcessedInstant](
      bucketName,
      filePathPrefix,
      stringHash(bucketName) + stringHash(filePathPrefix) + afterwards.instant.getEpochSecond.intValue,
      context.transducer,
      context.parallelism,
      context.pollingTimings,
      State(
        afterwards,
        getNextState(filePathPrefix, context.dateTimeFormatter.value),
        context.deriveKafkaKey,
        selectObjectForInstant(context.dateTimeFormatter)
      )
    )
}
