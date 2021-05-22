package tamer
package s3

import com.sksamuel.avro4s.Codec
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import tamer.kafka.KafkaConfig
import zio.{Has, ZIO}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.s3.S3
import zio.stream.{Transducer, ZTransducer}

import java.time.ZoneId
import java.time.format.DateTimeFormatter

class TamerS3SuffixDateFetcher[R <: Blocking with Clock with S3 with Has[KafkaConfig]] {

  def fetchAccordingToSuffixDate[K: Codec, V: Codec](
      bucketName: String,
      prefix: String,
      afterwards: LastProcessedInstant,
      context: TamerS3SuffixDateFetcher.Context[R, K, V]
  ): ZIO[R, TamerError, Unit] = {

    val setup =
      S3Setup.mkTimeBased[R, K, V](
        bucketName,
        prefix,
        afterwards,
        context
      )
    S3Tamer(setup).run
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
      pollingTimings: S3Setup.S3PollingTimings = S3Setup.S3PollingTimings(
        minimumIntervalForBucketFetch = 5.minutes,
        maximumIntervalForBucketFetch = 5.minutes
      )
  )
}
