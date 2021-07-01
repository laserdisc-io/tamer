package tamer

import java.net.http.HttpRequest

import sttp.capabilities.{Effect, WebSockets}
import sttp.capabilities.zio.ZioStreams
import sttp.client3.Request
import zio.{Has, Layer, Ref, Task}
import sttp.client3.httpclient.zio._
import sttp.client3.SttpBackendOptions

package object rest {
  type EphemeralSecretCache = Ref[Option[String]]
  type SttpRequest          = Request[Either[String, String], ZioStreams with Effect[Task] with WebSockets]

  final def restLive(
      options: SttpBackendOptions = SttpBackendOptions.Default,
      customizeRequest: HttpRequest => HttpRequest = identity,
      customEncodingHandler: HttpClientZioBackend.ZioEncodingHandler = PartialFunction.empty
  ): Layer[TamerError, SttpClient with Has[EphemeralSecretCache]] =
    HttpClientZioBackend
      .layer(options, customizeRequest, customEncodingHandler)
      .mapError(e => TamerError(e.getLocalizedMessage(), e)) ++ EphemeralSecretCache.live
}
