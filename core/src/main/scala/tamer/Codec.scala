package tamer

import java.io.{InputStream, OutputStream}

import io.confluent.kafka.schemaregistry.ParsedSchema

sealed trait Codec[A] {
  def decode: InputStream => Either[Throwable, A]
  def encode: (A, OutputStream) => Unit
  def schema: ParsedSchema
}

object Codec {
  final def apply[A](implicit A: Codec[A]): Codec[A] = A

  implicit final def avro4s[A](
      implicit decoderA: com.sksamuel.avro4s.Decoder[A],
      encoderA: com.sksamuel.avro4s.Encoder[A],
      schemaForA: com.sksamuel.avro4s.SchemaFor[A]
  ): Codec[A] = new Codec[A] {
    private[this] final val _avroDecoderBuilder = com.sksamuel.avro4s.AvroInputStream.binary(decoderA)
    private[this] final val _avroEncoderBuilder = com.sksamuel.avro4s.AvroOutputStream.binary(encoderA)
    private[this] final val _avroSchema         = schemaForA.schema

    override final val decode: InputStream => Either[Throwable, A] = _avroDecoderBuilder.from(_).build(_avroSchema).tryIterator.next().toEither
    override final val encode: (A, OutputStream) => Unit = (a, os) => {
      val ser = _avroEncoderBuilder.to(os).build()
      ser.write(a)
      ser.close()
    }
    override final val schema: ParsedSchema = new io.confluent.kafka.schemaregistry.avro.AvroSchema(_avroSchema)
  }
}
