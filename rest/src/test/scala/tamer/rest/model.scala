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

import java.util.UUID

import io.circe.{Codec => CirceCodec}
import io.circe.generic.semiauto._
import vulcan.{Codec => VulcanCodec}
import vulcan.generic._

case class Key(time: Long)
object Key {
  implicit val vulcanCodec: VulcanCodec[Key] = VulcanCodec.derive[Key]
}
case class Value(time: Long, uuid: UUID)
object Value {
  implicit val circeCodec: CirceCodec[Value]   = deriveCodec[Value]
  implicit val vulcanCodec: VulcanCodec[Value] = VulcanCodec.derive[Value]
}
case class State(count: Int)
object State {
  implicit val hashable: Hashable[State]       = _.count
  implicit val vulcanCodec: VulcanCodec[State] = VulcanCodec.derive[State]
}
