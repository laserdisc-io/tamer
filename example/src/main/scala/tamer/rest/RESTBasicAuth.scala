package tamer
package rest

import sttp.client4.RetryWhen
import zio._

object RESTBasicAuth extends ZIOAppDefault {
  import implicits._

  val dataRegex = """.*"data":"(-?[\d]+).*""".r
  val pageDecoder: String => Task[DecodedPage[MyData, Offset]] =
    DecodedPage.fromString {
      case dataRegex(data) => ZIO.attempt(List(MyData(data.toInt)))
      case pageBody        => ZIO.fail(new RuntimeException(s"Could not parse pageBody: $pageBody"))
    }

  def retrySchedule(
      request: SttpRequest
  ): Schedule[Any, FallibleResponse, FallibleResponse] =
    Schedule.spaced(5.seconds) *> Schedule.recurs(3) *> Schedule.recurWhile(response => RetryWhen.Default(request, response))

  override final val run = RESTSetup
    .paginated(
      baseUrl = "http://localhost:9395/basic-auth",
      pageDecoder = pageDecoder,
      authentication = Some(Authentication.basic("user", "pass")),
      retrySchedule = Some(retrySchedule)
    )(
      recordFrom = (_, data) => Record(MyKey(data.i), data),
      offsetParameterName = "offset",
      increment = 2
    )
    .runWith(restLive() ++ KafkaConfig.fromEnvironment)
}
