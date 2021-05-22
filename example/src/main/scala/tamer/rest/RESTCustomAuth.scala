package tamer
package rest

import sttp.client3.httpclient.zio.{HttpClientZioBackend, SttpClient, send}
import sttp.client3.{UriContext, basicRequest}
import zio._

import scala.util.matching.Regex

object RESTCustomAuth extends App {
  import RESTTamer.Offset

  val httpClientLayer  = HttpClientZioBackend.layer()
  val kafkaConfigLayer = KafkaConfig.fromEnvironment
  val fullLayer        = httpClientLayer ++ kafkaConfigLayer ++ LocalSecretCache.live

  case class MyData(i: Int)

  object MyData {
    implicit val codec = AvroCodec.codec[MyData]
  }

  case class MyKey(i: Int)

  object MyKey {
    implicit val codec = AvroCodec.codec[MyKey]
  }

  val dataRegex: Regex = """.*"data":"(-?[\d]+).*""".r
  val pageDecoder: String => Task[DecodedPage[MyData, Offset]] = DecodedPage.fromString {
    case dataRegex(data) => Task(List(MyData(data.toInt)))
    case pageBody        => Task.fail(new RuntimeException(s"Could not parse pageBody: $pageBody"))
  }

  val authentication: Authentication[SttpClient] = new Authentication[SttpClient] {

    override def addAuthentication(request: SttpRequest, bearerToken: Option[String]): SttpRequest = request.auth.bearer(bearerToken.getOrElse(""))

    override def setSecret(secretRef: Ref[Option[String]]): ZIO[SttpClient, TamerError, Unit] = {
      val fetchToken = send(basicRequest.get(uri"http://localhost:9095/auth").auth.basic("user", "pass"))
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

  private val program = RESTTamer.withPagination(
    baseUrl = "http://localhost:9095",
    pageDecoder = pageDecoder,
    offsetParameterName = "offset",
    increment = 2,
    authenticationMethod = Some(authentication)
  )((_, data) => MyKey(data.i))

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = program.run.provideCustomLayer(fullLayer).exitCode
}
