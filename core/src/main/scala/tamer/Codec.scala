package tamer

import java.io.{InputStream, OutputStream}

import scala.annotation.{implicitNotFound, nowarn}

@implicitNotFound(
  "\n" +
    "Could not find or construct a \u001b[36mtamer.Codec\u001b[0m instance for type:\n" +
    "\n" +
    "  \u001b[32m${A}\u001b[0m\n" +
    "\n" +
    "This can happen for a few reasons, but the most common case is a(/some) missing implicit(/implicits).\n" +
    "\n" +
    "Specifically, you need to ensure that wherever you are expected to provide a \u001b[36mtamer.Codec[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m:\n" +
    "  1. If *either* one of the following Avro libraries is in the classpath:\n" +
    "    a. Vulcan\n" +
    "    b. Avro4s\n" +
    "  then, respectively:\n" +
    "    a. A \u001b[36mvulcan.Codec[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m must be in scope too\n" +
    "    b. A \u001b[36mcom.sksamuel.avro4s.Decoder[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m, a \u001b[36mcom.sksamuel.avro4s.Encoder[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m, and a \u001b[36mcom.sksamuel.avro4s.SchemaFor[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m must be in scope too.\n" +
    "  2. Alternatively, if *either* one of the following is in the classpath:\n" +
    "    c. Circe\n" +
    "    d. Jsoniter Scala\n" +
    "    e. ZIO Json\n" +
    "    then, respectively:\n" +
    "    c. An \u001b[36mio.circe.Decoder[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m, and an \u001b[36mio.circe.Encoder[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m must be in scope too\n" +
    "    d. A \u001b[36mcom.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m must be in scope too\n" +
    "    e. A \u001b[36mzio.json.JsonCodec[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m must be in scope too.\n" +
    "\n" +
    "Given how implicit resolution works in Scala, and more importantly how these implicits are defined in Tamer, care must be taken to avoid ambiguity when multiple Avro or Json libraries are in the classpath concurrently.\n" +
    "To cater for this scenario, it is sufficient to explicitly summon the expected underlying \u001b[36mtamer.Codec\u001b[0m's instance provider, that is:\n" +
    "  a. Vulcan: `import \u001b[36mtamer.Codec.optionalVulcanCodec[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m`\n" +
    "  b. Avro4s: `import \u001b[36mtamer.Codec.optionalAvro4sCodec[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m`\n" +
    "  c. Circe: `import \u001b[36mtamer.Codec.optionalCirceCodec[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m`\n" +
    "  d. Jsoniter Scala: `import \u001b[36mtamer.Codec.optionalJsoniterScalaCodec[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m`\n" +
    "  e. ZIO Json: `import \u001b[36mtamer.Codec.optionalZioJsonCodec[\u001b[32m${A}\u001b[0m\u001b[36m]\u001b[0m`.\n"
)
sealed trait Codec[@specialized A] {
  type S
  def decode(is: InputStream): A
  def encode(value: A, os: OutputStream): Unit
  def maybeSchema: Option[S]
}

// The series of tricks used to summon implicit instances using optional dependencies
// was proposed by Kai and Pavel Shirshov in https://blog.7mind.io/no-more-orphans.html
object Codec extends LowPriorityCodecs {
  final def apply[A](implicit A: Codec[A]): Codec[A] = A

  implicit final def optionalVulcanCodec[A, C[_]: VulcanCodec](implicit ca: C[A]): Codec[A] = new AvroCodec[A] {
    private[this] final val _vulcanCodec = ca.asInstanceOf[vulcan.Codec[A]]

    override final val schema: org.apache.avro.Schema =
      _vulcanCodec.schema.getOrElse(throw new ExceptionInInitializerError(s"Schema is not valid"))

    override final def decode(is: InputStream): A = {
      val decoder = org.apache.avro.io.DecoderFactory.get.binaryDecoder(is, null)
      val value   = new org.apache.avro.generic.GenericDatumReader[Any](schema).read(null, decoder)
      _vulcanCodec.decode(value, schema) match {
        case Left(error)  => throw error.throwable
        case Right(value) => value
      }
    }
    override final def encode(value: A, os: OutputStream): Unit = _vulcanCodec.encode(value) match {
      case Left(error) => throw error.throwable
      case Right(encoded) =>
        val encoder = org.apache.avro.io.EncoderFactory.get.binaryEncoder(os, null)
        new org.apache.avro.generic.GenericDatumWriter[Any](schema).write(encoded, encoder)
        encoder.flush()
    }
  }

  implicit final def optionalAvro4sCodec[A, D[_]: Avro4sDecoder, E[_]: Avro4sEncoder, SF[_]: Avro4sSchemaFor](
      implicit da: D[A],
      ea: E[A],
      sfa: SF[A]
  ): Codec[A] = new AvroCodec[A] {
    private[this] final val _avroDecoderBuilder = com.sksamuel.avro4s.AvroInputStream.binary(da.asInstanceOf[com.sksamuel.avro4s.Decoder[A]])
    private[this] final val _avroEncoderBuilder = com.sksamuel.avro4s.AvroOutputStream.binary(ea.asInstanceOf[com.sksamuel.avro4s.Encoder[A]])

    override final val schema: org.apache.avro.Schema = sfa.asInstanceOf[com.sksamuel.avro4s.SchemaFor[A]].schema

    override final def decode(is: InputStream): A = _avroDecoderBuilder.from(is).build(schema).iterator.next()
    override final def encode(value: A, os: OutputStream): Unit = {
      val ser = _avroEncoderBuilder.to(os).build()
      ser.write(value)
      ser.close()
    }
  }
}
private[tamer] sealed trait LowPriorityCodecs extends LowestPriorityCodecs {
  implicit final def optionalCirceCodec[A, D[_]: CirceDecoder, E[_]: CirceEncoder](implicit da: D[A], ea: E[A]): Codec[A] = new SchemalessCodec[A] {
    private[this] final val _circeDecoder = da.asInstanceOf[io.circe.Decoder[A]]
    private[this] final val _circeEncoder = ea.asInstanceOf[io.circe.Encoder[A]]

    override final def decode(is: InputStream): A = io.circe.jawn.decodeChannel(java.nio.channels.Channels.newChannel(is))(_circeDecoder) match {
      case Left(error)  => throw error
      case Right(value) => value
    }
    override final def encode(value: A, os: OutputStream): Unit =
      new java.io.OutputStreamWriter(os, java.nio.charset.StandardCharsets.UTF_8).append(_circeEncoder(value).noSpaces).flush()
  }

  implicit final def optionalJsoniterScalaCodec[@specialized A, C[_]: JsoniterScalaCodec](implicit ca: C[A]): Codec[A] = new SchemalessCodec[A] {
    private[this] final val _jsoniterCodec = ca.asInstanceOf[com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec[A]]

    override final def decode(is: InputStream): A               = com.github.plokhotnyuk.jsoniter_scala.core.readFromStream(is)(_jsoniterCodec)
    override final def encode(value: A, os: OutputStream): Unit = com.github.plokhotnyuk.jsoniter_scala.core.writeToStream(value, os)(_jsoniterCodec)
  }

  implicit final def optionalZioJsonCodec[A, C[_]: ZioJsonCodec](implicit ca: C[A]): Codec[A] = new SchemalessCodec[A] {
    private[this] final val _zioJsonCodec = ca.asInstanceOf[zio.json.JsonCodec[A]]

    override final def decode(is: InputStream): A = zio.Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.default.unsafe.run(_zioJsonCodec.decoder.decodeJsonStreamInput(zio.stream.ZStream.fromInputStream(is))).getOrThrowFiberFailure()
    }
    override final def encode(value: A, os: OutputStream): Unit =
      new java.io.OutputStreamWriter(os).append(_zioJsonCodec.encodeJson(value, None)).flush()
  }
}
private[tamer] sealed trait LowestPriorityCodecs {
  private[tamer] sealed abstract class AvroCodec[@specialized A] extends Codec[A] {
    override final type S = org.apache.avro.Schema
    def schema: S
    override final def maybeSchema: Option[S] = Some(schema)
  }
  private[tamer] sealed abstract class SchemalessCodec[@specialized A] extends Codec[A] {
    override final type S = Nothing
    override final val maybeSchema: Option[S] = None
  }
  final type Aux[@specialized A, S0] = Codec[A] { type S = S0 }
}

private final abstract class VulcanCodec[C[_]]
@nowarn private object VulcanCodec { @inline implicit final def get: VulcanCodec[vulcan.Codec] = null }

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

private final abstract class JsoniterScalaCodec[C[_]]
@nowarn private object JsoniterScalaCodec {
  @inline implicit final def get: JsoniterScalaCodec[com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec] = null
}

private final abstract class ZioJsonCodec[C[_]]
@nowarn private object ZioJsonCodec { @inline implicit final def get: ZioJsonCodec[zio.json.JsonCodec] = null }
