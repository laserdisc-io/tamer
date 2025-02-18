/*
 * Copyright (c) 2019-2025 LaserDisc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package tamer
package db

import fs2.Chunk

import java.time.Instant
import scala.util.hashing.byteswap64

final case class ResultMetadata(queryExecutionTimeInNanos: Long)
final case class QueryResult[V](metadata: ResultMetadata, results: List[V])
final case class ChunkWithMetadata[V](chunk: Chunk[V], pulledAt: Long = System.nanoTime())
final case class ValueWithMetadata[V](value: V, pulledAt: Long = System.nanoTime())

/** By specifying a field here, tamer will order database records according to this date. Usually you want your latest update timestamp here.
  * @param timestamp
  *   the value tamer will use to order the records by.
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
