package tamer
package s3

import software.amazon.awssdk.regions.Region
import tamer.kafka.KafkaConfig
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.s3.S3

import java.net.URI
import java.time.{Instant, ZoneId}

object S3Simple extends App {
  private val myProgram: ZIO[Blocking with Clock with S3 with Has[KafkaConfig], TamerError, Unit] =
    new TamerS3SuffixDateFetcher[Blocking with Clock with S3 with Has[KafkaConfig]]()
      .fetchAccordingToSuffixDate(
        bucketName = "myBucket",
        prefix = "myFolder/myPrefix",
        afterwards = LastProcessedInstant(Instant.parse("2020-12-03T10:15:30.00Z")),
        context = TamerS3SuffixDateFetcher.Context(
          dateTimeFormatter = ZonedDateTimeFormatter.fromPattern("yyyy-MM-dd HH:mm:ss", ZoneId.of("Europe/Rome"))
        )
      )
      .unit

  private lazy val myS3Layer   = s3.liveM(Region.AF_SOUTH_1, s3.providers.default, Some(new URI("http://localhost:9000")))
  private lazy val myFullLayer = myS3Layer ++ KafkaConfig.fromEnvironment

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = myProgram.provideCustomLayer(myFullLayer).exitCode
}
