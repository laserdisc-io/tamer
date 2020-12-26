package tamer
package db

import fs2.Chunk

import java.time.Instant

object Db {
  abstract class Datable(val instant: Instant)

  case class ChunkWithMetadata[V](chunk: Chunk[V], pulledAt: Instant = Instant.now())
  case class ValueWithMetadata[V](value: V, pulledAt: Instant = Instant.now())

  case class TimeSegment(from: Instant, to: Instant) extends State {
    override lazy val stateId: Int = this.hashCode()
  }
}
