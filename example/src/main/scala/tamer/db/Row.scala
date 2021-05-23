package tamer
package db

import java.time.Instant

final case class Row(id: String, name: String, description: Option[String], modifiedAt: Instant) extends Timestamped(modifiedAt)
