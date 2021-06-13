package tamer
package db

import fs2.Chunk

import java.time.Instant
import scala.util.hashing.byteswap64

final case class ResultMetadata(queryExecutionTimeInNanos: Long)
final case class QueryResult[V](metadata: ResultMetadata, results: List[V])
final case class ChunkWithMetadata[V](chunk: Chunk[V], pulledAt: Long = System.nanoTime())
final case class ValueWithMetadata[V](value: V, pulledAt: Long = System.nanoTime())

/** By specifying a field here, tamer will order database records according
  * to this date. Usually you want your latest update timestamp here.
  * @param timestamp the value tamer will use to order the records by.
  */
abstract class Timestamped(val timestamp: Instant)
object Timestamped {
  implicit final def ordering[A <: Timestamped]: Ordering[A] =
    (x: A, y: A) => Ordering[Instant].compare(x.timestamp, y.timestamp)
}

final case class Window(from: Instant, to: Instant)
object Window {
  implicit final val hashable: Hashable[Window] = s => (byteswap64(s.from.getEpochSecond) + byteswap64(s.to.getEpochSecond)).intValue
}
