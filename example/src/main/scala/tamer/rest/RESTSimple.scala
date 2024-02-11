package tamer
package rest

import zio._

object RESTSimple extends ZIOAppDefault {
  import implicits._

  private[this] final val pageDecoder: String => Task[DecodedPage[String, Offset]] =
    DecodedPage.fromString { body =>
      ZIO.attempt(body.split(",").toList.filterNot(_.isBlank))
    }

  override final val run = RESTSetup
    .paginated(
      baseUrl = "http://localhost:9395/finite-pagination",
      pageDecoder = pageDecoder
    )(
      recordFrom = (_, data) => Record(data, data),
      fixedPageElementCount = Some(3)
    )
    .runWith(restLive() ++ KafkaConfig.fromEnvironment)
}
