package tamer.db

import fs2.Chunk

object Compat {
  def toIterable[V](chunk: Chunk[V]): Stream[V] =
    chunk.iterator.to[Stream]
}
