package tamer

import java.io.{InputStream, OutputStream}

import io.confluent.kafka.schemaregistry.ParsedSchema
import scala.annotation.{implicitNotFound, nowarn}

@implicitNotFound("Could not find an implicit Codec[${A}] in scope")
sealed trait Codec[A] {
  def decode(is: InputStream): Either[Throwable, A]
  def encode(value: A, os: OutputStream): Unit
  def maybeSchema: Option[ParsedSchema]
}

// The series of tricks used to summon implicit instances using optional dependencies
// was proposed by Kai and Pavel Shirshov in https://blog.7mind.io/no-more-orphans.html
object Codec extends LowPriorityCodecs {
  final def apply[A](implicit A: Codec[A]): Codec[A] = A

  implicit final def optionalAvro4sCodec[A, D[_]: Avro4sDecoder, E[_]: Avro4sEncoder, SF[_]: Avro4sSchemaFor](
      implicit da: D[A],
      ea: E[A],
      sfa: SF[A]
  ): Codec[A] = new Codec[A] {
    private[this] final val _avroDecoderBuilder = com.sksamuel.avro4s.AvroInputStream.binary(da.asInstanceOf[com.sksamuel.avro4s.Decoder[A]])
    private[this] final val _avroEncoderBuilder = com.sksamuel.avro4s.AvroOutputStream.binary(ea.asInstanceOf[com.sksamuel.avro4s.Encoder[A]])
    private[this] final val _avroSchema         = sfa.asInstanceOf[com.sksamuel.avro4s.SchemaFor[A]].schema

    override final def decode(is: InputStream): Either[Throwable, A] = _avroDecoderBuilder.from(is).build(_avroSchema).tryIterator.next().toEither
    override final def encode(value: A, os: OutputStream): Unit = {
      val ser = _avroEncoderBuilder.to(os).build()
      ser.write(value)
      ser.close()
    }
    override final val maybeSchema: Option[ParsedSchema] = Some(new io.confluent.kafka.schemaregistry.avro.AvroSchema(_avroSchema))
  }
}
private[tamer] sealed trait LowPriorityCodecs {
  implicit final def optionalCirceCodec[A, D[_]: CirceDecoder, E[_]: CirceEncoder](
      implicit da: D[A],
      ea: E[A]
  ): Codec[A] = new Codec[A] {
    private[this] final val _circeDecoder = da.asInstanceOf[io.circe.Decoder[A]]
    private[this] final val _circeEncoder = ea.asInstanceOf[io.circe.Encoder[A]]

    override final def decode(is: InputStream): Either[Throwable, A] =
      io.circe.jawn.decodeChannel(java.nio.channels.Channels.newChannel(is))(_circeDecoder)
    override final def encode(value: A, os: OutputStream): Unit =
      new java.io.OutputStreamWriter(os, java.nio.charset.StandardCharsets.UTF_8).append(_circeEncoder(value).noSpaces).flush()
    override final val maybeSchema: Option[ParsedSchema] = None
  }

  implicit final def optionalZioJsonCodec[A, D[_]: ZioJsonDecoder, E[_]: ZioJsonEncoder](
      implicit da: D[A],
      ea: E[A]
  ): Codec[A] = new Codec[A] {
    private[this] final val _zioJsonDecoder = da.asInstanceOf[zio.json.JsonDecoder[A]]
    private[this] final val _zioJsonEncoder = ea.asInstanceOf[zio.json.JsonEncoder[A]]

    override final def decode(is: InputStream): Either[Throwable, A] =
      zio.Runtime.default.unsafeRun(_zioJsonDecoder.decodeJsonStreamInput(zio.stream.ZStream.fromInputStream(is)).either)
    override final def encode(value: A, os: OutputStream): Unit =
      new java.io.OutputStreamWriter(os).append(_zioJsonEncoder.encodeJson(value, None)).flush()
    override final val maybeSchema: Option[ParsedSchema] = None
  }
}

private final abstract class Avro4sDecoder[D[_]]
@nowarn private object Avro4sDecoder { @inline implicit final def get: Avro4sDecoder[com.sksamuel.avro4s.Decoder] = null }
private final abstract class Avro4sEncoder[E[_]]
@nowarn private object Avro4sEncoder { @inline implicit final def get: Avro4sEncoder[com.sksamuel.avro4s.Encoder] = null }
private final abstract class Avro4sSchemaFor[SF[_]]
@nowarn private object Avro4sSchemaFor { @inline implicit final def get: Avro4sSchemaFor[com.sksamuel.avro4s.SchemaFor] = null }

private final abstract class CirceDecoder[D[_]]
@nowarn private object CirceDecoder { @inline implicit final def get: CirceDecoder[io.circe.Decoder] = null }
private final abstract class CirceEncoder[E[_]]
@nowarn private object CirceEncoder { @inline implicit final def get: CirceEncoder[io.circe.Encoder] = null }

private final abstract class ZioJsonDecoder[D[_]]
@nowarn private object ZioJsonDecoder { @inline implicit final def get: ZioJsonDecoder[zio.json.JsonDecoder] = null }
private final abstract class ZioJsonEncoder[E[_]]
@nowarn private object ZioJsonEncoder { @inline implicit final def get: ZioJsonEncoder[zio.json.JsonEncoder] = null }
