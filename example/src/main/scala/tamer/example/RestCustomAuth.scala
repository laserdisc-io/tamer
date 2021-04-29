package tamer.example

import sttp.client3.httpclient.zio.{HttpClientZioBackend, SttpClient, send}
import sttp.client3.{UriContext, basicRequest}
import tamer.config.{Config, KafkaConfig}
import tamer.rest.TamerRestJob.Offset
import tamer.rest.{Authentication, DecodedPage, LocalSecretCache, SttpRequest, TamerRestJob}
import tamer.{AvroCodec, TamerError}
import zio._

import scala.util.matching.Regex

object RestCustomAuth extends App {
  val httpClientLayer: RLayer[ZEnv, SttpClient] =
    HttpClientZioBackend.layer()
  val kafkaConfigLayer: Layer[TamerError, KafkaConfig] = Config.live
  val fullLayer: RLayer[ZEnv, SttpClient with KafkaConfig with Has[Ref[Option[String]]]] =
    httpClientLayer ++ kafkaConfigLayer ++ LocalSecretCache.live

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

  private val program = TamerRestJob.withPagination(
    baseUrl = "http://localhost:9095",
    pageDecoder = pageDecoder,
    offsetParameterName = "offset",
    increment = 2,
    authenticationMethod = Some(authentication)
  )((_, data) => MyKey(data.i))

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = program.fetch().provideCustomLayer(fullLayer).exitCode
}
