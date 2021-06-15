package tamer

import java.io.{InputStream, OutputStream}

import com.sksamuel.avro4s._
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema

sealed trait Codec[A] {
  def decode: InputStream => Either[Throwable, A]
  def encode: (A, OutputStream) => Unit
  def schema: ParsedSchema
}

object Codec {
  final def apply[A](implicit A: Codec[A]): Codec[A] = A

  implicit final def avro4s[A: Decoder: Encoder: SchemaFor]: Codec[A] = new Codec[A] {
    private[this] final val _avroSchema                            = SchemaFor[A].schema
    private[this] final val _avroDecoder                           = AvroInputStream.binary[A](Decoder[A])
    private[this] final val _avroEncoder                           = AvroOutputStream.binary[A](Encoder[A])
    override final val decode: InputStream => Either[Throwable, A] = _avroDecoder.from(_).build(_avroSchema).tryIterator.next().toEither
    override final val encode: (A, OutputStream) => Unit = (a, os) => {
      val ser = _avroEncoder.to(os).build()
      ser.write(a)
      ser.close()
    }
    override final val schema: ParsedSchema = new AvroSchema(_avroSchema)
  }
}
