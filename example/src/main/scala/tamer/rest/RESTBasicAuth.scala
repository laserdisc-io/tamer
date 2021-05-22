package tamer
package rest

import sttp.client3.httpclient.zio.HttpClientZioBackend
import tamer.kafka.KafkaConfig
import tamer.rest.RESTTamer.Offset
import zio._

object RESTBasicAuth extends App {
  val sttpLayer        = HttpClientZioBackend.layer()
  val kafkaConfigLayer = KafkaConfig.fromEnvironment
  val fullLayer        = sttpLayer ++ kafkaConfigLayer ++ LocalSecretCache.live

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

  private val program = RESTTamer.withPagination(
    baseUrl = "http://localhost:9095/basic-auth",
    pageDecoder = pageDecoder,
    offsetParameterName = "offset",
    increment = 2,
    authenticationMethod = Authentication.basic("user", "pass")
  )((_, data) => MyKey(data.i))

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = program.run.provideCustomLayer(fullLayer).exitCode
}
