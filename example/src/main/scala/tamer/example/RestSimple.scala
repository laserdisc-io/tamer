package tamer.example
import sttp.client3.httpclient.zio.{HttpClientZioBackend, SttpClient}
import tamer.config.{Config, KafkaConfig}
import tamer.rest.TamerRestJob.Offset
import tamer.rest.{DecodedPage, TamerRestJob}
import tamer.{AvroCodec, TamerError}
import zio.{ExitCode, Layer, RIO, Task, URIO, ZEnv, ZLayer}

object RestSimple extends zio.App {
  val httpClientLayer: ZLayer[ZEnv, Throwable, SttpClient] =
    HttpClientZioBackend.layer()
  val kafkaConfigLayer: Layer[TamerError, KafkaConfig]                = Config.live
  val fullLayer: ZLayer[ZEnv, Throwable, SttpClient with KafkaConfig] = httpClientLayer ++ kafkaConfigLayer

  case class MyData(i: Int)
  object MyData {
    implicit val codec = AvroCodec.codec[MyData]
  }

  case class MyKey(i: Int)
  object MyKey {
    implicit val codec = AvroCodec.codec[MyKey]
  }

  val pageDecoder: String => RIO[Any, DecodedPage[MyData, Offset]] = DecodedPage.fromString { pageBody =>
    val dataRegex = """"data":"(-?[\d]+)"""".r
    pageBody match {
      case dataRegex(data) => Task(List(MyData(data.toInt)))
      case _ => Task.fail(new RuntimeException(s"Could not parse pageBody: $pageBody"))
    }
  }

  private val program = TamerRestJob.withPagination(
    baseUrl = "localhost:9095",
    pageDecoder = pageDecoder,
    offsetParameterName = "offset",
    increment = 2
  )((_, data) => MyKey(data.i))

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.fetch().provideCustomLayer(fullLayer).exitCode
}
