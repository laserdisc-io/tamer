package tamer
package db

import fs2.Chunk

import java.time.Instant
import scala.util.hashing.byteswap64

object Db {
  abstract class Datable(val instant: Instant)
  object Datable {
    val underlyingOrdering: Ordering[Instant]                    = implicitly[Ordering[Instant]]
    implicit def ordering[Subtype <: Datable]: Ordering[Subtype] = (x: Datable, y: Datable) => underlyingOrdering.compare(x.instant, y.instant)
  }

  case class ChunkWithMetadata[V](chunk: Chunk[V], pulledAt: Instant = Instant.now())
  case class ValueWithMetadata[V](value: V, pulledAt: Instant = Instant.now())

  case class TimeSegment(from: Instant, to: Instant) extends HashableState {
    override lazy val stateHash: Int = (byteswap64(from.getEpochSecond) + byteswap64(to.getEpochSecond)).intValue
  }
}
