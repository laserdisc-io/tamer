package tamer
package rest

import sttp.client3.httpclient.zio.HttpClientZioBackend
import tamer.kafka.KafkaConfig
import tamer.rest.RESTTamer.Offset
import zio._

import scala.annotation.nowarn

object RESTSimple extends App {
  val httpClientLayer  = HttpClientZioBackend.layer()
  val kafkaConfigLayer = KafkaConfig.fromEnvironment
  val fullLayer        = httpClientLayer ++ kafkaConfigLayer ++ LocalSecretCache.live

  @nowarn val pageDecoder: String => Task[DecodedPage[String, Offset]] =
    DecodedPage.fromString { body =>
      Task(body.split(",").toList.filterNot(_.isBlank))
    }

  implicit val stringCodec = AvroCodec.codec[String]

  private val program = RESTTamer.withPagination(
    baseUrl = "http://localhost:9095/finite-pagination",
    pageDecoder = pageDecoder,
    fixedPageElementCount = Some(3)
  )((_, data) => data)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = program.run.provideCustomLayer(fullLayer).exitCode
}
