package tamer
package rest

import sttp.client3.httpclient.zio.HttpClientZioBackend
import tamer.kafka.KafkaConfig
import tamer.rest.RESTTamer.PeriodicOffset
import zio._

import java.time.Instant

object RestDynamicData extends App {
  val httpClientLayer  = HttpClientZioBackend.layer()
  val kafkaConfigLayer = KafkaConfig.fromEnvironment
  val fullLayer        = httpClientLayer ++ kafkaConfigLayer ++ LocalSecretCache.live

  val pageDecoder: String => Task[DecodedPage[String, PeriodicOffset]] = // TODO: select automatically type according to helper method
    DecodedPage.fromString { body =>
      Task(body.split(",").toList.filterNot(_.isBlank))
    }

  implicit val stringCodec = AvroCodec.codec[String]

  private def program(now: Instant) = RESTTamer.withPaginationPeriodic(
    baseUrl = "http://localhost:9095/dynamic-pagination",
    pageDecoder = pageDecoder,
    periodStart = now
  )((_, data) => data)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    clock.instant.flatMap(now => program(now).run.provideCustomLayer(fullLayer).exitCode)
}
