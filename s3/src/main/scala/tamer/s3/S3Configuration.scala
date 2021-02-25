package tamer
package s3

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import eu.timepit.refined.types.numeric.PosInt
import zio.stream.ZTransducer
import zio.{Queue, UIO, ZIO}

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant}
import scala.util.hashing.MurmurHash3.stringHash


final case class S3Configuration[
  K <: Product : Encoder : Decoder : SchemaFor,
  V <: Product : Encoder : Decoder : SchemaFor,
  S <: Product : Decoder : Encoder : SchemaFor
](
   bucketName: String,
   prefix: String,
   override val tamerStateKafkaRecordKey: Int,
   transducer: ZTransducer[Any, TamerError, Byte, V],
   parallelism: PosInt,
   timeouts: S3Configuration.Timeouts,
   transitions: S3Configuration.StateTransitions[K, V, S]
 ) extends _root_.tamer.Setup[K, V, S](
  Setup.SourceSerde[K, V, S](),
  defaultState = transitions.initialState,
  tamerStateKafkaRecordKey = tamerStateKafkaRecordKey
)

object S3Configuration {

  case class Timeouts(
                       minimumIntervalForBucketFetch: Duration,
                       maximumIntervalForBucketFetch: Duration,
                     )

  case class StateTransitions[K, V, S](
                                        initialState: S,
                                        getNextState: (KeysR, S, Queue[Unit]) => UIO[S],
                                        deriveKafkaRecordKey: (S, V) => K,
                                        selectObjectForState: (S, Keys) => Option[String]
                                      )

  private[s3] final def suffixWithoutFileExtension(key: String, prefix: String, dateTimeFormatter: DateTimeFormatter): String = {
    val dotCountInDate = dateTimeFormatter.format(Instant.EPOCH).count(_ == '.')
    val keyWithoutExtension =
      if (key.count(_ == '.') > dotCountInDate) key.split('.').splitAt(dotCountInDate + 1)._1.mkString(".") else key
    keyWithoutExtension.stripPrefix(prefix)
  }

  private[s3] final def parseInstantFromKey(key: String, prefix: String, dateTimeFormatter: DateTimeFormatter): Instant =
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

  private[s3] final def getNextState(prefix: String, dateTimeFormatter: DateTimeFormatter)(
    keysR: KeysR,
    afterwards: LastProcessedInstant,
    keysChangedToken: Queue[Unit]
  ): UIO[LastProcessedInstant] = {
    val retryAfterWaitingForKeyListChange =
      keysChangedToken.take *> getNextState(prefix, dateTimeFormatter)(keysR, afterwards, keysChangedToken)
    getNextInstant(keysR, afterwards, prefix, dateTimeFormatter)
      .flatMap {
        case Some(newInstant) if newInstant.isAfter(afterwards.instant) => UIO(LastProcessedInstant(newInstant))
        case _ => retryAfterWaitingForKeyListChange
      }
  }

  private final def selectObjectForInstant(
                                            zonedDateTimeFormatter: ZonedDateTimeFormatter
                                          )(lastProcessedInstant: LastProcessedInstant, keys: Keys): Option[String] =
    keys.find(_.contains(zonedDateTimeFormatter.value.format(lastProcessedInstant.instant)))

  final def mkTimeBased[K <: Product : Encoder : Decoder : SchemaFor, V <: Product : Encoder : Decoder : SchemaFor](
                                                                                                                     bucketName: String,
                                                                                                                     filePathPrefix: String,
                                                                                                                     afterwards: LastProcessedInstant,
                                                                                                                     context: TamerS3SuffixDateFetcher.Context[K, V]
                                                                                                                   ): S3Configuration[K, V, LastProcessedInstant] = S3Configuration[K, V, LastProcessedInstant](
    bucketName,
    filePathPrefix,
    stringHash(bucketName) + stringHash(filePathPrefix) + afterwards.instant.getEpochSecond.intValue,
    context.transducer,
    context.parallelism,
    Timeouts(
      context.minimumIntervalForBucketFetch,
      context.maximumIntervalForBucketFetch,
    ),
    StateTransitions(
      afterwards,
      getNextState(filePathPrefix, context.dateTimeFormatter.value),
      context.deriveKafkaKey,
      selectObjectForInstant(context.dateTimeFormatter)
    ),
  )
}
