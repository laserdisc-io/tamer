package tamer
package rest

import zio._

import scala.annotation.nowarn

object RESTSimple extends App {
  import implicits._

  @nowarn val pageDecoder: String => Task[DecodedPage[String, Offset]] =
    DecodedPage.fromString { body =>
      Task(body.split(",").toList.filterNot(_.isBlank))
    }

  val program: ZIO[ZEnv, TamerError, Unit] = RESTSetup
    .paginated(
      baseUrl = "http://localhost:9395/finite-pagination",
      pageDecoder = pageDecoder
    )(
      recordKey = (_, data) => data,
      fixedPageElementCount = Some(3)
    )
    .runWith(restLive() ++ kafkaConfigFromEnvironment)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}
