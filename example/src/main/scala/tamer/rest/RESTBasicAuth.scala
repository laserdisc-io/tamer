package tamer
package rest

import sttp.client3.RetryWhen
import zio._
import zio.duration._

object RESTBasicAuth extends App {
  case class MyKey(i: Int)
  case class MyData(i: Int)

  val dataRegex = """.*"data":"(-?[\d]+).*""".r
  val pageDecoder: String => Task[DecodedPage[MyData, Offset]] =
    DecodedPage.fromString {
      case dataRegex(data) => Task(List(MyData(data.toInt)))
      case pageBody        => Task.fail(new RuntimeException(s"Could not parse pageBody: $pageBody"))
    }

  def retrySchedule(
      request: SttpRequest
  ): Schedule[Any, FallibleResponse, FallibleResponse] =
    Schedule.spaced(5.seconds) *> Schedule.recurs(3) *> Schedule.recurWhile(response => RetryWhen.Default(request, response))

  val program: ZIO[ZEnv, TamerError, Unit] = RESTSetup
    .paginated(
      baseUrl = "http://localhost:9095/basic-auth",
      pageDecoder = pageDecoder,
      authentication = Some(Authentication.basic("user", "pass")),
      retrySchedule = Some(retrySchedule)
    )(
      recordKey = (_, data) => MyKey(data.i),
      offsetParameterName = "offset",
      increment = 2
    )
    .runWith(restLive() ++ kafkaConfigFromEnvironment)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}
