package tamer
package rest

import vulcan.Codec
import vulcan.generic._

object implicits {
  implicit final val stateKeyVulcanCodec: Codec[Tamer.StateKey]       = Codec.derive[Tamer.StateKey]
  implicit final val offsetVulcanCodec: Codec[Offset]                 = Codec.derive[Offset]
  implicit final val periodicOffsetVulcanCodec: Codec[PeriodicOffset] = Codec.derive[PeriodicOffset]
}
