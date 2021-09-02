package tamer

import java.io.{InputStream, OutputStream}

import io.confluent.kafka.schemaregistry.ParsedSchema
import scala.annotation.{implicitNotFound, nowarn}

@implicitNotFound("Could not find an implicit Codec[${A}] in scope")
sealed trait Codec[@specialized A] {
  def decode(is: InputStream): A
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

    override final def decode(is: InputStream): A = _avroDecoderBuilder.from(is).build(_avroSchema).iterator.next()
    override final def encode(value: A, os: OutputStream): Unit = {
      val ser = _avroEncoderBuilder.to(os).build()
      ser.write(value)
      ser.close()
    }
    override final val maybeSchema: Option[ParsedSchema] = Some(new io.confluent.kafka.schemaregistry.avro.AvroSchema(_avroSchema))
  }
}
private[tamer] sealed trait LowPriorityCodecs {
  implicit final def optionalCirceCodec[A, C[_]: CirceCodec](implicit ca: C[A]): Codec[A] = new Codec[A] {
    private[this] final val _circeCodec = ca.asInstanceOf[io.circe.Codec[A]]

    override final def decode(is: InputStream): A = io.circe.jawn.decodeChannel(java.nio.channels.Channels.newChannel(is))(_circeCodec) match {
      case Left(error)  => throw error
      case Right(value) => value
    }
    override final def encode(value: A, os: OutputStream): Unit =
      new java.io.OutputStreamWriter(os, java.nio.charset.StandardCharsets.UTF_8).append(_circeCodec(value).noSpaces).flush()
    override final val maybeSchema: Option[ParsedSchema] = None
  }

  implicit final def optionalJsoniterScalaCodec[@specialized A, C[_]: JsoniterScalaCodec](implicit ca: C[A]): Codec[A] = new Codec[A] {
    private[this] final val _jsoniterCodec = ca.asInstanceOf[com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec[A]]

    override final def decode(is: InputStream): A               = com.github.plokhotnyuk.jsoniter_scala.core.readFromStream(is)(_jsoniterCodec)
    override final def encode(value: A, os: OutputStream): Unit = com.github.plokhotnyuk.jsoniter_scala.core.writeToStream(value, os)(_jsoniterCodec)
    override final val maybeSchema: Option[ParsedSchema]        = None
  }

  implicit final def optionalZioJsonCodec[A, C[_]: ZioJsonCodec](implicit ca: C[A]): Codec[A] = new Codec[A] {
    private[this] final val _zioJsonCodec = ca.asInstanceOf[zio.json.JsonCodec[A]]

    override final def decode(is: InputStream): A =
      zio.Runtime.default.unsafeRun(_zioJsonCodec.decodeJsonStreamInput(zio.stream.ZStream.fromInputStream(is)))
    override final def encode(value: A, os: OutputStream): Unit =
      new java.io.OutputStreamWriter(os).append(_zioJsonCodec.encodeJson(value, None)).flush()
    override final val maybeSchema: Option[ParsedSchema] = None
  }
}

private final abstract class Avro4sDecoder[D[_]]
@nowarn private object Avro4sDecoder { @inline implicit final def get: Avro4sDecoder[com.sksamuel.avro4s.Decoder] = null }
private final abstract class Avro4sEncoder[E[_]]
@nowarn private object Avro4sEncoder { @inline implicit final def get: Avro4sEncoder[com.sksamuel.avro4s.Encoder] = null }
private final abstract class Avro4sSchemaFor[SF[_]]
@nowarn private object Avro4sSchemaFor { @inline implicit final def get: Avro4sSchemaFor[com.sksamuel.avro4s.SchemaFor] = null }

private final abstract class CirceCodec[C[_]]
@nowarn private object CirceCodec { @inline implicit final def get: CirceCodec[io.circe.Codec] = null }

private final abstract class JsoniterScalaCodec[C[_]]
@nowarn private object JsoniterScalaCodec {
  @inline implicit final def get: JsoniterScalaCodec[com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec] = null
}

private final abstract class ZioJsonCodec[C[_]]
@nowarn private object ZioJsonCodec { @inline implicit final def get: ZioJsonCodec[zio.json.JsonCodec] = null }
