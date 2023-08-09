package tamer
package s3

import vulcan.Codec
import vulcan.generic._

object implicits {
  implicit final val stateKeyVulcanCodec: Codec[Tamer.StateKey] = Codec.derive[Tamer.StateKey]
}
