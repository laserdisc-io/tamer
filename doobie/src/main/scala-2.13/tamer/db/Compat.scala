package tamer
package db

import fs2.Chunk

object Compat {
  def toIterable[V](chunk: Chunk[V]): LazyList[V] =
    chunk.iterator.to(LazyList)
}
