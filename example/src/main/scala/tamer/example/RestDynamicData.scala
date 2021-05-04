package tamer.example

import sttp.client3.httpclient.zio.{HttpClientZioBackend, SttpClient}
import tamer.config.{Config, KafkaConfig}
import tamer.rest.LocalSecretCache.LocalSecretCache
import tamer.rest.TamerRestJob.PeriodicOffset
import tamer.rest.{DecodedPage, LocalSecretCache, TamerRestJob}
import tamer.{AvroCodec, TamerError}
import zio._

import java.time.Instant
import scala.annotation.nowarn

object RestDynamicData extends App {
  val httpClientLayer: RLayer[ZEnv, SttpClient] =
    HttpClientZioBackend.layer()
  val kafkaConfigLayer: Layer[TamerError, KafkaConfig] = Config.live
  val fullLayer: RLayer[ZEnv, SttpClient with KafkaConfig with LocalSecretCache] =
    httpClientLayer ++ kafkaConfigLayer ++ LocalSecretCache.live

  @nowarn
  val pageDecoder: String => Task[DecodedPage[String, PeriodicOffset]] = // TODO: select automatically type according to helper method
    DecodedPage.fromString { body =>
      Task(body.split(",").toList.filterNot(_.isBlank))
    }

  implicit val stringCodec = AvroCodec.codec[String]

  private def program(now: Instant) = TamerRestJob.withPaginationPeriodic(
    baseUrl = "http://localhost:9095/dynamic-pagination",
    pageDecoder = pageDecoder,
    periodStart = now
  )((_, data) => data)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    clock.instant.flatMap(now => program(now).fetch().provideCustomLayer(fullLayer).exitCode)
}
