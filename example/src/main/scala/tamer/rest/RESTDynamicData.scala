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

import zio._

object RESTDynamicData extends ZIOAppDefault {
  import implicits._

  val pageDecoder: String => Task[DecodedPage[String, PeriodicOffset]] = // TODO: select automatically type according to helper method
    DecodedPage.fromString { body =>
      ZIO.attempt(body.split(",").toList.filterNot(_.isBlank))
    }

  override final val run = Clock.instant.flatMap { now =>
    RESTSetup
      .periodicallyPaginated(
        baseUrl = "http://localhost:9395/dynamic-pagination",
        pageDecoder = pageDecoder,
        periodStart = now
      )((_, data) => Record(data, data))
      .runWith(restLive() ++ KafkaConfig.fromEnvironment)
  }
}
