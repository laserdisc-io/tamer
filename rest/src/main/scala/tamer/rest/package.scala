/*
 * Copyright (c) 2019-2025 LaserDisc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package tamer

import java.net.http.HttpRequest
import sttp.capabilities.zio.ZioStreams
import sttp.client4.{BackendOptions, Request, Response}
import sttp.client4.compression.CompressionHandlers
import sttp.client4.httpclient.zio._
import zio.{Ref, TaskLayer}

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
