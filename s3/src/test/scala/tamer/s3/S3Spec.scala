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

import zio.{Queue, Ref}
import zio.test._
import zio.test.Assertion._

import java.time.format.DateTimeFormatterBuilder
import java.time.{ZoneId, ZonedDateTime}

object S3Spec extends ZIOSpecDefault {

  private[this] final val rome = ZoneId.of("Europe/Rome")

  override final val spec = suite("S3Spec")(
    test("Should be able to compute next state") {
      val from      = ZonedDateTime.parse("2021-01-01T00:01:43+01:00[Europe/Rome]").toInstant
      val formatter = ZonedDateTimeFormatter(new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter(), rome)
      val prefix    = "myFolder/myPrefix"
      val instant   = ZonedDateTime.parse("2021-01-01T00:01:44+01:00[Europe/Rome]").toInstant
      val makeKeys  = Ref.make(List("myFolder/myPrefix2021-01-01 00:01:44.empty"))
      val makeQueue = Queue.dropping[Unit](requestedCapacity = 1).tap(_.offer(()))

      (makeQueue <*> makeKeys).flatMap { case (keysQ, keysR) =>
        assertZIO(S3Setup.getNextState(prefix, formatter)(keysR, from, keysQ))(equalTo(instant))
      }
    }
  )
}
