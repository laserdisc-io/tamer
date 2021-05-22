package tamer
package db

import java.time.Instant

final case class Row(id: String, name: String, description: Option[String], modifiedAt: Instant) extends Timestamped(modifiedAt)
object Row {
  implicit final val codec = AvroCodec.codec[Row]
}
