package tamer.example

import software.amazon.awssdk.regions.Region
import tamer.TamerError
import tamer.s3.{LastProcessedInstant, ZonedDateTimeFormatter}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.s3.{ConnectionError, InvalidCredentials, S3, S3Credentials}
import zio.{ExitCode, Layer, URIO, ZIO}

import java.net.URI
import java.time.{Instant, ZoneId}

object S3Simple extends zio.App {
  val mkS3Layer: ZIO[Blocking, InvalidCredentials, Layer[ConnectionError, S3]] =
    S3Credentials.fromAll.map(s3Credentials => zio.s3.live(Region.AF_SOUTH_1, s3Credentials, Some(new URI("http://localhost:9000"))))

  val program: ZIO[Blocking with Clock with S3, TamerError, Unit] = for {
    _ <- tamer.s3.fetchAccordingToSuffixDate(
      bucketName = "myBucket",
      prefix = "myFolder/myPrefix",
      afterwards = LastProcessedInstant(Instant.parse("2020-12-03T10:15:30.00Z")),
      dateTimeFormatter = ZonedDateTimeFormatter.fromPattern("yyyy-MM-dd HH:mm:ss", ZoneId.of("Europe/Rome"))
    )
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = mkS3Layer.flatMap(s3Layer => program.provideCustomLayer(s3Layer)).exitCode
}
