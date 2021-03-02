package tamer

import sttp.capabilities.WebSockets
import sttp.capabilities.zio.ZioStreams
import sttp.client3.SttpBackend
import zio._

package object rest {
  type KeysR = Ref[List[String]]
  type Keys  = List[String]

  type SttpClient = Has[SttpBackend[Task, ZioStreams with WebSockets]]
}
