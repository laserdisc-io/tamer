package tamer
package db

import vulcan.Codec
import vulcan.generic._

object implicits {
  implicit final val stateKeyVulcanCodec: Codec[Tamer.StateKey] = Codec.derive[Tamer.StateKey]
  implicit final val windowVulcanCodec: Codec[Window] = Codec.derive[Window]
}
