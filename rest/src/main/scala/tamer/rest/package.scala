package tamer

import sttp.capabilities.{Effect, WebSockets}
import sttp.capabilities.zio.ZioStreams
import sttp.client3.Request
import zio.Task

package object rest {
  type SttpRequest = Request[Either[String, String], ZioStreams with Effect[Task] with WebSockets]
}
