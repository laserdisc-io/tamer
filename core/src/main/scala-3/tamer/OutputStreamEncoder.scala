package tamer

object OutputStreamEncoder {
  def avro4sOutputStream[A](
      schema: org.apache.avro.Schema,
      encoderA: com.sksamuel.avro4s.Encoder[A]
  ): com.sksamuel.avro4s.AvroOutputStreamBuilder[A] =
    com.sksamuel.avro4s.AvroOutputStream.binary(schema, encoderA)
}
