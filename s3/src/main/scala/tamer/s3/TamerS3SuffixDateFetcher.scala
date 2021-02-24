package tamer.s3

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import eu.timepit.refined.types.numeric.PosInt
import tamer.TamerError
import tamer.config.{Config, KafkaConfig}
import tamer.kafka.Kafka
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.stream.{Transducer, ZTransducer}
import zio.{Layer, Task, ZIO}
import eu.timepit.refined.auto._
import java.time.{Duration, ZoneId}
import java.time.format.DateTimeFormatter


class TamerS3SuffixDateFetcher(tamerS3: TamerS3) {
  private val defaultTransducer: Transducer[Nothing, Byte, Line] =
    ZTransducer.utf8Decode >>> ZTransducer.splitLines.map(Line)

  private final def kafkaLayer(kafkaConfigLayer: Layer[TamerError, KafkaConfig]): Layer[TamerError, Kafka] = kafkaConfigLayer >>> Kafka.live

  def fetchAccordingToSuffixDate[R,
    K <: Product : Encoder : Decoder : SchemaFor,
    V <: Product : Encoder : Decoder : SchemaFor
  ](
     bucketName: String,
     prefix: String,
     afterwards: LastProcessedInstant,
     deriveKafkaKey: (LastProcessedInstant, V) => K = (l: LastProcessedInstant, _: V) => l,
     transducer: ZTransducer[Any, TamerError, Byte, V] = defaultTransducer,
     parallelism: PosInt = 1,
     dateTimeFormatter: ZonedDateTimeFormatter = ZonedDateTimeFormatter(DateTimeFormatter.ISO_INSTANT, ZoneId.systemDefault()),
     minimumIntervalForBucketFetch: Duration = 5.minutes,
     maximumIntervalForBucketFetch: Duration = 5.minutes,
     kafkaConfig: Task[Config.Kafka] = ZIO.service[Config.Kafka].provideLayer(Config.live)
   ): ZIO[R with Blocking with Clock with zio.s3.S3, TamerError, Unit] = {
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
    tamerS3.fetch(setup).provideSomeLayer[R with Blocking with Clock with zio.s3.S3](
      kafkaLayer(kafkaConfig.toLayer.mapError(e => TamerError("Error while fetching default kafka configuration", e)))
    )
  }

}
