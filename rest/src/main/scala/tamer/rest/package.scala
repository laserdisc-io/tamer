package tamer

import java.net.http.HttpRequest
import sttp.capabilities.{Effect, WebSockets}
import sttp.capabilities.zio.ZioStreams
import sttp.client4.{BackendOptions, Request, Response}
import sttp.client4.compression.CompressionHandlers
import sttp.client4.httpclient.zio._
import zio.{Ref, Task, TaskLayer}

package object rest {
  type EphemeralSecretCache = Ref[Option[String]]
  type SttpRequest          = Request[Either[String, String]]
  type FallibleResponse     = Either[Throwable, Response[Either[String, String]]]

  final def restLive(
      options: BackendOptions = BackendOptions.Default,
      customizeRequest: HttpRequest => HttpRequest = identity,
      customCompressionHandler: CompressionHandlers[ZioStreams, ZioStreams.BinaryStream] = HttpClientZioBackend.DefaultCompressionHandlers
  ): TaskLayer[SttpClient with EphemeralSecretCache] =
    HttpClientZioBackend
      .layer(options, customizeRequest, customCompressionHandler)
      .mapError(e => TamerError(e.getLocalizedMessage(), e)) ++ EphemeralSecretCache.live
}
