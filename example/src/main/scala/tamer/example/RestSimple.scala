package tamer.example
import sttp.client3.httpclient.zio.{HttpClientZioBackend, SttpClient}
import tamer.{AvroCodec, TamerError}
import tamer.config.{Config, KafkaConfig}
import tamer.rest.TamerRestJob
import zio.stream.ZTransducer
import zio.{ExitCode, Layer, URIO, ZEnv, ZLayer}

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

  val transducer = ZTransducer.utf8Decode.map(_.toInt).map(MyData(_))
  private val program = TamerRestJob.withPagination(
    baseUrl = "localhost:9095",
    transducer = transducer,
    offsetParameterName = "offset",
    increment = 2
  )((_, data) => MyKey(data.i))

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.fetch().provideCustomLayer(fullLayer).exitCode
}
