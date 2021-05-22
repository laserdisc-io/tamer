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
  implicit final def ordering[A <: Timestamped]: Ordering[A] =
    (x: A, y: A) => Ordering[Instant].compare(x.timestamp, y.timestamp)
}

case class ChunkWithMetadata[V](chunk: Chunk[V], pulledAt: Instant = Instant.now())
case class ValueWithMetadata[V](value: V, pulledAt: Instant = Instant.now())

case class TimeSegment(from: Instant, to: Instant)

object TimeSegment {
  implicit final val codec = AvroCodec.codec[TimeSegment]

  implicit final val hashable: Hashable[TimeSegment] = s => (byteswap64(s.from.getEpochSecond) + byteswap64(s.to.getEpochSecond)).intValue
}
