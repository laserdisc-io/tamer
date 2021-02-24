package tamer.s3

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import tamer.TamerError
import tamer.kafka.Kafka
import zio.ZIO
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.stream.{Transducer, ZTransducer}

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneId}


class TamerS3SuffixDateFetcher(tamerS3: TamerS3) {


  def fetchAccordingToSuffixDate[
    K <: Product : Encoder : Decoder : SchemaFor,
    V <: Product : Encoder : Decoder : SchemaFor
  ](
     bucketName: String,
     prefix: String,
     afterwards: LastProcessedInstant,
     context: TamerS3SuffixDateFetcher.Context[K, V],
   ): ZIO[Blocking with Clock with zio.s3.S3 with Kafka, TamerError, Unit] = {
    import context._

    val setup =
      Setup.mkTimeBased[K, V](
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
    tamerS3.fetch(setup)
  }

}

object TamerS3SuffixDateFetcher {
  private val defaultTransducer: Transducer[Nothing, Byte, Line] =
    ZTransducer.utf8Decode >>> ZTransducer.splitLines.map(Line)

  case class Context[K, V] (
                             deriveKafkaKey: (LastProcessedInstant, V) => K = (l: LastProcessedInstant, _: V) => l,
                             transducer: ZTransducer[Any, TamerError, Byte, V] = defaultTransducer,
                             parallelism: PosInt = 1,
                             dateTimeFormatter: ZonedDateTimeFormatter = ZonedDateTimeFormatter(DateTimeFormatter.ISO_INSTANT, ZoneId.systemDefault()),
                             minimumIntervalForBucketFetch: Duration = 5.minutes,
                             maximumIntervalForBucketFetch: Duration = 5.minutes,
                           )
}