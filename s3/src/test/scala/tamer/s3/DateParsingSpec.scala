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

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.format.FormatStyle.SHORT
import java.util.Locale.{GERMANY, ITALY}

import zio.test._
import zio.test.Assertion._

object DateParsingSpec extends ZIOSpecDefault {

  private[this] final val rome = ZoneId.of("Europe/Rome")

  override final val spec = suite("DateParsingSpec")(
    test("Should compute suffix") {
      val date      = LocalDateTime.parse("2021-01-01T00:01:44")
      val formatter = ZonedDateTimeFormatter(DateTimeFormatter.ofLocalizedDateTime(SHORT).localizedBy(ITALY), rome)
      val prefix    = "myFolder/myPrefix"
      val key       = s"myFolder/myPrefix${formatter.format(date)}.empty"

      assert(S3Setup.suffixWithoutFileExtension(key, prefix, formatter))(equalTo("01/01/21, 00:01"))
    },
    test("Should compute suffix even if complex") {
      val date      = LocalDateTime.parse("2021-01-01T00:01:44")
      val formatter = ZonedDateTimeFormatter(DateTimeFormatter.ofLocalizedDateTime(SHORT).localizedBy(GERMANY), rome)
      val prefix    = "myFolder/myPrefix"
      val key       = s"myFolder/myPrefix${formatter.format(date)}.empty.tar.gz"

      assert(S3Setup.suffixWithoutFileExtension(key, prefix, formatter))(equalTo("01.01.21, 00:01"))
    },
    test("Should parse date in simple case") {
      val prefix    = "myFolder/myPrefix"
      val key       = "myFolder/myPrefix2021-01-01 00:01:44.empty"
      val formatter = ZonedDateTimeFormatter(new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter(), rome)

      assert(S3Setup.parseInstantFromKey(key, prefix, formatter))(
        equalTo(ZonedDateTime.parse("2021-01-01T00:01:44+01:00[Europe/Rome]").toInstant)
      )
    }
  )
}
