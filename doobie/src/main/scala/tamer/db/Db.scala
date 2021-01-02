package tamer
package db

import fs2.Chunk

import java.time.Instant
import scala.util.hashing.byteswap64

/** By specifying a field here, tamer will order database records according
  * to this date. Usually you want your latest update timestamp here.
  * @param timestamp the value tamer will use to order the records by.
  */
abstract class Timestamped(val timestamp: Instant)
object Timestamped {
  val underlyingOrdering: Ordering[Instant] = implicitly[Ordering[Instant]]
  implicit def ordering[Subtype <: Timestamped]: Ordering[Subtype] =
    (x: Timestamped, y: Timestamped) => underlyingOrdering.compare(x.timestamp, y.timestamp)
}

case class ChunkWithMetadata[V](chunk: Chunk[V], pulledAt: Instant = Instant.now())
case class ValueWithMetadata[V](value: V, pulledAt: Instant = Instant.now())

case class TimeSegment(from: Instant, to: Instant) extends HashableState {
  override lazy val stateHash: Int = (byteswap64(from.getEpochSecond) + byteswap64(to.getEpochSecond)).intValue
}
