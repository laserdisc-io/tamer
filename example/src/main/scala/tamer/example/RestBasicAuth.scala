package tamer.example

import sttp.client3.httpclient.zio.{HttpClientZioBackend, SttpClient}
import tamer.config.{Config, KafkaConfig}
import tamer.rest.LocalSecretCache.LocalSecretCache
import tamer.rest.TamerRestJob.Offset
import tamer.rest.{Authentication, DecodedPage, LocalSecretCache, TamerRestJob}
import tamer.{AvroCodec, TamerError}
import zio._

object RestBasicAuth extends App {
  val httpClientLayer: RLayer[ZEnv, SttpClient] =
    HttpClientZioBackend.layer()
  val kafkaConfigLayer: Layer[TamerError, KafkaConfig] = Config.live
  val fullLayer: RLayer[ZEnv, SttpClient with KafkaConfig with LocalSecretCache] =
    httpClientLayer ++ kafkaConfigLayer ++ LocalSecretCache.live

  case class MyData(i: Int)

  object MyData {
    implicit val codec = AvroCodec.codec[MyData]
  }

  case class MyKey(i: Int)

  object MyKey {
    implicit val codec = AvroCodec.codec[MyKey]
  }

  val dataRegex = """.*"data":"(-?[\d]+).*""".r
  val pageDecoder: String => Task[DecodedPage[MyData, Offset]] =
    DecodedPage.fromString {
      case dataRegex(data) => Task(List(MyData(data.toInt)))
      case pageBody        => Task.fail(new RuntimeException(s"Could not parse pageBody: $pageBody"))
    }

  private val program = TamerRestJob.withPagination(
    baseUrl = "http://localhost:9095/basic-auth",
    pageDecoder = pageDecoder,
    offsetParameterName = "offset",
    increment = 2,
    authenticationMethod = Authentication.basic("user", "pass")
  )((_, data) => MyKey(data.i))

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = program.fetch().provideCustomLayer(fullLayer).exitCode
}
