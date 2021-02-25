package tamer.example

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.S3Exception
import tamer.TamerError
import tamer.kafka.Kafka
import tamer.s3.TamerS3.TamerS3Impl
import tamer.s3.{LastProcessedInstant, TamerS3, TamerS3SuffixDateFetcher, ZonedDateTimeFormatter}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.s3._
import zio.{ExitCode, Has, URIO, ZIO, ZLayer}

import java.net.URI
import java.time.{Instant, ZoneId}

object S3Simple extends zio.App {
  private val tamer: TamerS3 = new TamerS3Impl()

  private val program: ZIO[Blocking with Clock with S3 with Kafka, TamerError, Unit] = for {
    _ <- new TamerS3SuffixDateFetcher(tamer).fetchAccordingToSuffixDate(
      bucketName = "myBucket",
      prefix = "myFolder/myPrefix",
      afterwards = LastProcessedInstant(Instant.parse("2020-12-03T10:15:30.00Z")),
      context = TamerS3SuffixDateFetcher.Context(
        dateTimeFormatter = ZonedDateTimeFormatter.fromPattern("yyyy-MM-dd HH:mm:ss", ZoneId.of("Europe/Rome")),
      ),
    )
  } yield ()

  private lazy val credsLayer: ZLayer[Blocking, InvalidCredentials, Has[S3Credentials]] = ZLayer.fromEffect(S3Credentials.fromAll)
  private lazy val s3Layer: ZLayer[Has[S3Credentials], ConnectionError, S3] = ZLayer.fromServiceManaged(creds => zio.s3.live(Region.AF_SOUTH_1, creds, Some(new URI("http://localhost:9000"))).build.map(_.get))
  private lazy val fullS3Layer: ZLayer[Blocking, S3Exception, S3] = credsLayer >>> s3Layer
  private lazy val fullLayer: ZLayer[Blocking, RuntimeException, S3 with Kafka] = fullS3Layer ++ Kafka.configuredForLive

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(fullLayer).exitCode
}
