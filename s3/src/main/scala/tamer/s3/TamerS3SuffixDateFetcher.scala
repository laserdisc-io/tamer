package tamer.s3

import com.sksamuel.avro4s.{Codec, SchemaFor}
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import tamer.TamerError
import tamer.config.KafkaConfig
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.stream.{Transducer, ZTransducer}

import java.time.ZoneId
import java.time.format.DateTimeFormatter

class TamerS3SuffixDateFetcher[R <: Blocking with Clock with zio.s3.S3 with KafkaConfig]() {

  def fetchAccordingToSuffixDate[
      K <: Product: Codec: SchemaFor,
      V <: Product: Codec: SchemaFor
  ](
      bucketName: String,
      prefix: String,
      afterwards: LastProcessedInstant,
      context: TamerS3SuffixDateFetcher.Context[R, K, V]
  ): ZIO[R, TamerError, Unit] = {

    val setup =
      S3Configuration.mkTimeBased[R, K, V](
        bucketName,
        prefix,
        afterwards,
        context
      )
    TamerS3Job(setup).fetch()
  }

}

object TamerS3SuffixDateFetcher {
  private val defaultTransducer: Transducer[Nothing, Byte, Line] =
    ZTransducer.utf8Decode >>> ZTransducer.splitLines.map(Line.apply)

  case class Context[R, K, V](
      deriveKafkaKey: (LastProcessedInstant, V) => K = (l: LastProcessedInstant, _: V) => l,
      transducer: ZTransducer[R, TamerError, Byte, V] = defaultTransducer,
      parallelism: PosInt = 1,
      dateTimeFormatter: ZonedDateTimeFormatter = ZonedDateTimeFormatter(DateTimeFormatter.ISO_INSTANT, ZoneId.systemDefault()),
      pollingTimings: S3Configuration.S3PollingTimings = S3Configuration.S3PollingTimings(
        minimumIntervalForBucketFetch = 5.minutes,
        maximumIntervalForBucketFetch = 5.minutes
      )
  )
}
