package tamer
package s3

import java.net.URI
import java.time.{Instant, ZoneId}

import software.amazon.awssdk.regions.Region.AF_SOUTH_1
import zio._
import zio.s3._

object S3Simple extends ZIOAppDefault {
  import implicits._

  override final val run = S3Setup
    .timed(
      bucket = "myBucket",
      prefix = "myFolder/myPrefix",
      from = Instant.parse("2020-12-03T10:15:30.00Z"),
      dateTimeFormatter = ZonedDateTimeFormatter.fromPattern("yyyy-MM-dd HH:mm:ss", ZoneId.of("Europe/Rome"))
    )
    .runWith(liveZIO(AF_SOUTH_1, ZIO.scoped(s3.providers.default), Some(new URI("http://localhost:9000"))) ++ KafkaConfig.fromEnvironment)
}
