package tamer
package rest

import java.util.UUID

import io.circe.{Codec => CirceCodec}
import io.circe.generic.semiauto._
import vulcan.{Codec => VulcanCodec}
import vulcan.generic._

case class Key(time: Long)
object Key {
  implicit val vulcanCodec: VulcanCodec[Key] = VulcanCodec.derive[Key]
}
case class Value(time: Long, uuid: UUID)
object Value {
  implicit val circeCodec: CirceCodec[Value] = deriveCodec[Value]
  implicit val vulcanCodec: VulcanCodec[Value] = VulcanCodec.derive[Value]
}
case class State(count: Int)
object State {
  implicit val hashable: Hashable[State] = _.count
  implicit val vulcanCodec: VulcanCodec[State] = VulcanCodec.derive[State]
}
