package tamer
package db

import fs2.Chunk

object compat {
  implicit final class ChunkOps[A](private val chunk: Chunk[A]) extends AnyVal {
    final def toStream: Stream[A] = chunk.iterator.to[Stream]
  }
}
