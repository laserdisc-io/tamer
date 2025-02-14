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
package rest

import sttp.client4.RetryWhen
import zio._

object RESTBasicAuth extends ZIOAppDefault {
  import implicits._

  val dataRegex = """.*"data":"(-?[\d]+).*""".r
  val pageDecoder: String => Task[DecodedPage[MyData, Offset]] =
    DecodedPage.fromString {
      case dataRegex(data) => ZIO.attempt(List(MyData(data.toInt)))
      case pageBody        => ZIO.fail(new RuntimeException(s"Could not parse pageBody: $pageBody"))
    }

  def retrySchedule(
      request: SttpRequest
  ): Schedule[Any, FallibleResponse, FallibleResponse] =
    Schedule.spaced(5.seconds) *> Schedule.recurs(3) *> Schedule.recurWhile(response => RetryWhen.Default(request, response))

  override final val run = RESTSetup
    .paginated(
      baseUrl = "http://localhost:9395/basic-auth",
      pageDecoder = pageDecoder,
      authentication = Some(Authentication.basic("user", "pass")),
      retrySchedule = Some(retrySchedule)
    )(
      recordFrom = (_, data) => Record(MyKey(data.i), data),
      offsetParameterName = "offset",
      increment = 2
    )
    .runWith(restLive() ++ KafkaConfig.fromEnvironment)
}
