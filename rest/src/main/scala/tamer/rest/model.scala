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
package rest

import java.time.Instant

import zio._

import scala.annotation.unused

trait Authentication[-R] {
  def addAuthentication(request: SttpRequest, supplementalSecret: Option[String]): SttpRequest

  def setSecret(@unused secretRef: Ref[Option[String]]): RIO[R, Unit] = ZIO.unit
  // TODO: could return the value to set instead of unit

  def refreshSecret(secretRef: Ref[Option[String]]): RIO[R, Unit] = setSecret(secretRef)
}
object Authentication {
  def basic[R](username: String, password: String): Authentication[R] =
    (request: SttpRequest, _: Option[String]) => request.auth.basic(username, password)
}

final case class DecodedPage[V, S](data: List[V], nextState: Option[S])
object DecodedPage {
  def fromString[R, V, S](decoder: String => RIO[R, List[V]]): String => RIO[R, DecodedPage[V, S]] =
    decoder.andThen(_.map(DecodedPage(_, None)))
}

object EphemeralSecretCache {
  val live: ULayer[EphemeralSecretCache] = ZLayer(Ref.make[Option[String]](None))
}

case class Offset(offset: Int, nextIndex: Int) {
  def incrementedBy(increment: Int): Offset = this.copy(offset = offset + increment, nextIndex = 0)
  def nextIndex(index: Int): Offset         = this.copy(nextIndex = index)
}
object Offset {
  implicit final val hashable: Hashable[Offset] = s => s.offset * s.nextIndex
}

case class PeriodicOffset(offset: Int, periodStart: Instant) {
  def incrementedBy(increment: Int): PeriodicOffset = this.copy(offset = offset + increment)
}
object PeriodicOffset {
  implicit final val hashable: Hashable[PeriodicOffset] = s => s.offset * (s.periodStart.getEpochSecond + s.periodStart.getNano.longValue).toInt
}
