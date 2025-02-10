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

import java.time.Instant

import scala.util.hashing._

trait Hashable[S] {

  /** It is required for this hash to be consistent even across executions for the same semantic state. This is in contrast with the built-in
    * `hashCode` method.
    */
  def hash(s: S): Int
}

object Hashable extends HashableInstances0 {
  def apply[S](implicit h: Hashable[S]): Hashable[S] = h
}

sealed trait HashableInstances0 extends HashableInstances1 {
  implicit final val instantHashable: Hashable[Instant] = i => byteswap64(i.getEpochSecond()).toInt
  implicit final val stringHashable: Hashable[String]   = MurmurHash3.stringHash(_)
}

sealed trait HashableInstances1 {
  implicit final def default[A](implicit A: Hashing[A]): Hashable[A] = A.hash(_)
}
