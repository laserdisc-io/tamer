package tamer
package rest

import vulcan.Codec
import vulcan.generic._

case class MyKey(i: Int)
object MyKey {
  implicit final val vulcanCodec: Codec[MyKey] = Codec.derive[MyKey]
}
case class MyData(i: Int)
object MyData {
  implicit final val vulcanCodec: Codec[MyData] = Codec.derive[MyData]
}
