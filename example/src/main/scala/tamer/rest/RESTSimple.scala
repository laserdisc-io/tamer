package tamer
package rest

import zio._

import scala.annotation.nowarn

object RESTSimple extends App {
  @nowarn val pageDecoder: String => Task[DecodedPage[String, Offset]] =
    DecodedPage.fromString { body =>
      Task(body.split(",").toList.filterNot(_.isBlank))
    }

  val program: ZIO[ZEnv, TamerError, Unit] = RESTSetup
    .paginated(
      baseUrl = "http://localhost:9095/finite-pagination",
      pageDecoder = pageDecoder
    )(
      recordKey = (_, data) => data,
      fixedPageElementCount = Some(3)
    )
    .runWith(restLive() ++ kafkaConfigAndRegistryFromEnvironment)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}
