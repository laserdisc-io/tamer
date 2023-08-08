package tamer
package oci.objectstorage

import vulcan.Codec
import vulcan.generic._

case class ObjectsCursor(startAfter: Option[String], current: Option[String])
object ObjectsCursor {
  implicit final val vulcanCodec: Codec[ObjectsCursor] = Codec.derive[ObjectsCursor]
}
