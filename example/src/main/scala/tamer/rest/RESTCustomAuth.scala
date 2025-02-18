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

import sttp.client4.httpclient.zio.{SttpClient, send}
import sttp.client4.{UriContext, basicRequest}
import zio._

import scala.util.matching.Regex

object RESTCustomAuth extends ZIOAppDefault {
  import implicits._

  val dataRegex: Regex = """.*"data":"(-?[\d]+).*""".r
  val pageDecoder: String => Task[DecodedPage[MyData, Offset]] = DecodedPage.fromString {
    case dataRegex(data) => ZIO.attempt(List(MyData(data.toInt)))
    case pageBody        => ZIO.fail(new RuntimeException(s"Could not parse pageBody: $pageBody"))
  }

  val authentication: Authentication[SttpClient] = new Authentication[SttpClient] {
    override def addAuthentication(request: SttpRequest, bearerToken: Option[String]): SttpRequest = request.auth.bearer(bearerToken.getOrElse(""))
    override def setSecret(secretRef: Ref[Option[String]]): RIO[SttpClient, Unit] = {
      val fetchToken = send(basicRequest.get(uri"http://localhost:9395/auth").auth.basic("user", "pass"))
        .flatMap(_.body match {
          case Left(error)  => ZIO.fail(TamerError(error))
          case Right(token) => ZIO.succeed(token)
        })
        .mapError(TamerError("Error while fetching token", _))
      for {
        token <- fetchToken
        _     <- secretRef.set(Some(token))
      } yield ()
    }
  }

  override final val run = RESTSetup
    .paginated(
      baseUrl = "http://localhost:9395",
      pageDecoder = pageDecoder,
      authentication = Some(authentication)
    )(
      recordFrom = (_, data) => Record(MyKey(data.i), data),
      offsetParameterName = "offset",
      increment = 2
    )
    .runWith(restLive() ++ KafkaConfig.fromEnvironment)
}
