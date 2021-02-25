package tamer
package s3

import java.time.Instant

final case class Line(str: String)
object Line {
  implicit val codec = AvroCodec.codec[Line]
}

final case class LastProcessedInstant(instant: Instant)

object LastProcessedInstant  {
  implicit val codec = AvroCodec.codec[LastProcessedInstant]

}
