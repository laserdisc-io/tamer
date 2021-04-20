package tamer.example

import sttp.client3.httpclient.zio.{HttpClientZioBackend, SttpClient, send}
import sttp.client3.{UriContext, basicRequest}
import tamer.config.{Config, KafkaConfig}
import tamer.rest.TamerRestJob.Offset
import tamer.rest.{Authentication, DecodedPage, SttpRequest, TamerRestJob}
import tamer.{AvroCodec, TamerError}
import zio.{ExitCode, Has, Layer, RIO, Ref, Task, UIO, URIO, ZEnv, ZIO, ZLayer}

object RestSimple extends zio.App {
  val httpClientLayer: ZLayer[ZEnv, Throwable, SttpClient] =
    HttpClientZioBackend.layer()
  val kafkaConfigLayer: Layer[TamerError, KafkaConfig] = Config.live
  val localStateLayer: ZLayer[Any, Nothing, Has[Ref[Option[String]]]] = ZLayer.fromEffect(Ref.make(Option.empty[String])) // TODO: use type alias
  val fullLayer: ZLayer[ZEnv, Throwable, SttpClient with KafkaConfig with Has[Ref[Option[String]]]] = httpClientLayer ++ kafkaConfigLayer ++ localStateLayer // TODO: use type alias

  case class MyData(i: Int)

  object MyData {
    implicit val codec = AvroCodec.codec[MyData]
  }

  case class MyKey(i: Int)

  object MyKey {
    implicit val codec = AvroCodec.codec[MyKey]
  }

  val pageDecoder: String => RIO[Any, DecodedPage[MyData, Offset]] = DecodedPage.fromString { pageBody =>
    val dataRegex = """.*"data":"(-?[\d]+).*""".r
    pageBody match {
      case dataRegex(data) => Task(List(MyData(data.toInt)))
      case _ => Task.fail(new RuntimeException(s"Could not parse pageBody: $pageBody"))
    }
  }

  val authentication: Authentication[SttpClient] = new Authentication[SttpClient] {

    override def addAuthentication(request: SttpRequest, bearerToken: String): SttpRequest = request.auth.bearer(bearerToken)

    override def setSecret(secretRef: Ref[Option[String]]): ZIO[SttpClient, TamerError, Unit] = {
      val fetchToken = send(basicRequest.get(uri"http://localhost:9095/auth").auth.basic("user", "pass")).flatMap(_.body match {
        case Left(error) => ZIO.fail(TamerError(error))
        case Right(token) => ZIO.succeed(token) <* UIO(println("-" * 200 + s"\nNEW TOKEN! ($token)\n" + "-" * 200))
      }).mapError(throwable => TamerError("Error while fetching token", throwable))
      for {
        token <- fetchToken
        _ <- secretRef.set(Some(token))
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

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.fetch().provideCustomLayer(fullLayer).exitCode
}
