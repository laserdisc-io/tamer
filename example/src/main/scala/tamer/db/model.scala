package tamer
package db

import java.time.Instant

import vulcan.Codec
import vulcan.generic._

final case class Row(id: String, name: String, description: Option[String], modifiedAt: Instant) extends Timestamped(modifiedAt)
object Row {
  implicit final val vulcanCodec: Codec[Row] = Codec.derive[Row]
}
final case class MyState(from: Instant, to: Instant)
object MyState {
  implicit final val hashable: Hashable[MyState] = s => s.from.hash + s.to.hash
  implicit final val vulcanCodec: Codec[MyState] = Codec.derive[MyState]
}