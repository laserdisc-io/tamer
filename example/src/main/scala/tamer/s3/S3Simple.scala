/*
 * Copyright (c) 2019-2025 LaserDisc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

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
