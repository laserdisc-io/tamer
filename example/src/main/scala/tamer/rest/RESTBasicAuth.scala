package tamer
package rest

import zio._

object RESTBasicAuth extends App {
  case class MyKey(i: Int)
  case class MyData(i: Int)

  val dataRegex = """.*"data":"(-?[\d]+).*""".r
  val pageDecoder: String => Task[DecodedPage[MyData, Offset]] =
    DecodedPage.fromString {
      case dataRegex(data) => Task(List(MyData(data.toInt)))
      case pageBody        => Task.fail(new RuntimeException(s"Could not parse pageBody: $pageBody"))
    }

  val program: ZIO[ZEnv, TamerError, Unit] = RESTSetup
    .paginated(
      baseUrl = "http://localhost:9095/basic-auth",
      pageDecoder = pageDecoder,
      offsetParameterName = "offset",
      increment = 2,
      authenticationMethod = Some(Authentication.basic("user", "pass"))
    )((_, data) => MyKey(data.i))
    .runWith(restLive() ++ kafkaConfigFromEnvironment)

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}
