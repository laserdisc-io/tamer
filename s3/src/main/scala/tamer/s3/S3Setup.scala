package tamer
package s3

import eu.timepit.refined.types.numeric.PosInt
import zio.stream.ZTransducer
import zio.{Queue, UIO, ZIO}

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant}
import scala.util.hashing.MurmurHash3.stringHash

final case class S3Setup[R, K: Codec, V: Codec, S: Codec](
    bucketName: String,
    prefix: String,
    tamerStateKafkaRecordKey: Int,
    transducer: ZTransducer[R, TamerError, Byte, V],
    parallelism: PosInt,
    pollingTimings: S3Setup.S3PollingTimings,
    transitions: S3Setup.State[K, V, S]
) {
  val generic: Setup[K, V, S] = Setup(Setup.Serdes[K, V, S], transitions.initialState, tamerStateKafkaRecordKey)
}

object S3Setup {

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
        case _                                                          => retryAfterWaitingForKeyListChange
      }
  }

  private final def selectObjectForInstant(
      zonedDateTimeFormatter: ZonedDateTimeFormatter
  )(lastProcessedInstant: LastProcessedInstant, keys: Keys): Option[String] =
    keys.find(_.contains(zonedDateTimeFormatter.value.format(lastProcessedInstant.instant)))

  final def mkTimeBased[R, K: Codec, V: Codec](
      bucketName: String,
      filePathPrefix: String,
      afterwards: LastProcessedInstant,
      context: TamerS3SuffixDateFetcher.Context[R, K, V]
  ): S3Setup[R, K, V, LastProcessedInstant] =
    S3Setup[R, K, V, LastProcessedInstant](
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
