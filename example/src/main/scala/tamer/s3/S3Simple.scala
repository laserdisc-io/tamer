package tamer
package s3

import java.net.URI
import java.time.{Instant, ZoneId}

import software.amazon.awssdk.regions.Region.AF_SOUTH_1
import zio._
import zio.s3._

object S3Simple extends App {
  import implicits._

  val program: ZIO[ZEnv, RuntimeException, Unit] = S3Setup
    .timed(
      bucket = "myBucket",
      prefix = "myFolder/myPrefix",
      from = Instant.parse("2020-12-03T10:15:30.00Z"),
      dateTimeFormatter = ZonedDateTimeFormatter.fromPattern("yyyy-MM-dd HH:mm:ss", ZoneId.of("Europe/Rome"))
    )
    .runWith(liveM(AF_SOUTH_1, s3.providers.default, Some(new URI("http://localhost:9000"))) ++ kafkaConfigFromEnvironment)

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}
