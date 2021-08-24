package tamer
package rest

import zio._

import java.time.Instant
import scala.annotation.nowarn

object RESTDynamicData extends App {
  @nowarn val pageDecoder: String => Task[DecodedPage[String, PeriodicOffset]] = // TODO: select automatically type according to helper method
    DecodedPage.fromString { body =>
      Task(body.split(",").toList.filterNot(_.isBlank))
    }

  def program(now: Instant): ZIO[ZEnv, TamerError, Unit] = RESTSetup
    .periodicallyPaginated(
      baseUrl = "http://localhost:9095/dynamic-pagination",
      pageDecoder = pageDecoder,
      periodStart = now
    )((_, data) => data)
    .runWith(restLive() ++ kafkaConfigAndRegistryFromEnvironment)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = clock.instant.flatMap(program(_).exitCode)
}
