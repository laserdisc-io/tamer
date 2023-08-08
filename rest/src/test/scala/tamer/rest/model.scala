package tamer
package rest

import java.util.UUID

import io.circe.Codec
import io.circe.generic.semiauto._

case class Key(time: Long)
case class Value(time: Long, uuid: UUID)
object Value {
  implicit val circeCodec: Codec[Value] = deriveCodec[Value]
}
case class State(count: Int)
object State {
  implicit val hashable: Hashable[State] = _.count
}
