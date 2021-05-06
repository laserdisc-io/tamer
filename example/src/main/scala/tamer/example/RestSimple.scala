package tamer.example

import sttp.client3.httpclient.zio.{HttpClientZioBackend, SttpClient}
import tamer.{AvroCodec, TamerError}
import tamer.config.{Config, KafkaConfig}
import tamer.rest.LocalSecretCache.LocalSecretCache
import tamer.rest.TamerRestJob.Offset
import tamer.rest.{DecodedPage, LocalSecretCache, TamerRestJob}
import zio._

import scala.annotation.nowarn

object RestSimple extends App {
  val httpClientLayer: RLayer[ZEnv, SttpClient] =
    HttpClientZioBackend.layer()
  val kafkaConfigLayer: Layer[TamerError, KafkaConfig] = Config.live
  val fullLayer: RLayer[ZEnv, SttpClient with KafkaConfig with LocalSecretCache] =
    httpClientLayer ++ kafkaConfigLayer ++ LocalSecretCache.live

  @nowarn
  val pageDecoder: String => Task[DecodedPage[String, Offset]] =
    DecodedPage.fromString { body =>
      Task(body.split(",").toList.filterNot(_.isBlank))
    }

  implicit val stringCodec = AvroCodec.codec[String]

  private val program = TamerRestJob.withPagination(
    baseUrl = "http://localhost:9095/finite-pagination",
    pageDecoder = pageDecoder,
    fixedPageElementCount = Some(3)
  )((_, data) => data)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = program.fetch().provideCustomLayer(fullLayer).exitCode
}
