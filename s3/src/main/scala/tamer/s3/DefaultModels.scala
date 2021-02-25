package tamer
package s3

import com.sksamuel.avro4s.Codec

import java.time.Instant

final case class Line(str: String)

final case class LastProcessedInstant(instant: Instant)

object LastProcessedInstantCodec {
  val c: Codec[LastProcessedInstant] = AvroEncodable.toCodec

}

object LastProcessedInstant {
  implicit val c: Codec[LastProcessedInstant] = LastProcessedInstantCodec.c

}
