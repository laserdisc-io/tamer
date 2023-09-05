package tamer
package rest

import sttp.client3.httpclient.zio.{SttpClient, send}
import sttp.client3.{UriContext, basicRequest}
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
    override def setSecret(secretRef: Ref[Option[String]]): ZIO[SttpClient, TamerError, Unit] = {
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
      recordKey = (_, data) => MyKey(data.i),
      offsetParameterName = "offset",
      increment = 2
    )
    .runWith(restLive() ++ KafkaConfig.fromEnvironment)
    .exitCode
}
