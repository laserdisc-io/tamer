package tamer
package rest

import java.time.Instant

import zio._

import scala.annotation.nowarn

object RESTDynamicData extends ZIOAppDefault {
  import implicits._

  @nowarn val pageDecoder: String => Task[DecodedPage[String, PeriodicOffset]] = // TODO: select automatically type according to helper method
    DecodedPage.fromString { body =>
      ZIO.attempt(body.split(",").toList.filterNot(_.isBlank))
    }

  def program(now: Instant) = RESTSetup
    .periodicallyPaginated(
      baseUrl = "http://localhost:9395/dynamic-pagination",
      pageDecoder = pageDecoder,
      periodStart = now
    )((_, data) => data)
    .runWith(restLive() ++ KafkaConfig.fromEnvironment)
    .exitCode

  override final val run = Clock.instant.flatMap(program(_))
}
