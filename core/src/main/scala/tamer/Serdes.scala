package tamer

import com.sksamuel.avro4s._
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import zio.kafka.serde.{Deserializer, Serializer}
import zio.{Has, RIO, Task, URIO}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer

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

sealed trait Serde[A] extends Any {
  def isKey: Boolean
  def schema: ParsedSchema
  def deserializer: Deserializer[RegistryInfo, A]
  def serializer: Serializer[RegistryInfo, A]
  final def serde: ZSerde[RegistryInfo, A] = ZSerde(deserializer)(serializer)
}

object Serde {
  private[this] final val Magic: Byte = 0x0
  private[this] final val intByteSize = 4

  final def key[A: Codec]   = new DelegatingSerde[A](isKey = true, Codec[A])
  final def value[A: Codec] = new DelegatingSerde[A](isKey = false, Codec[A])

  final class DelegatingSerde[A](override final val isKey: Boolean, codec: Codec[A]) extends Serde[A] {
    private[this] def subject(topic: String): String = s"$topic-${if (isKey) "key" else "value"}"

    override final val schema: ParsedSchema = codec.schema

    override final val deserializer: Deserializer[RegistryInfo, A] = Deserializer.byteArray.mapM { ba =>
      val buffer = ByteBuffer.wrap(ba)
      if (buffer.get() != Magic) RIO.fail(TamerError("Deserialization failed: unknown magic byte!"))
      else {
        val id = buffer.getInt()
        for {
          _ <- RIO.accessM[Has[Registry]](_.get.verifySchema(id, schema))
          res <- RIO.fromEither {
            val length  = buffer.limit() - (intByteSize + 1)
            val payload = new Array[Byte](length)
            buffer.get(payload, 0, length)
            codec.decode(new ByteArrayInputStream(payload))
          }
        } yield res
      }
    }
    override final val serializer: Serializer[RegistryInfo, A] = Serializer.byteArray.contramapM { a =>
      for {
        t  <- URIO.service[TopicName]
        id <- RIO.accessM[Has[Registry]](_.get.getOrRegisterId(subject(t), schema))
        arr <- Task {
          val baos = new ByteArrayOutputStream
          baos.write(ByteBuffer.allocate(intByteSize + 1).put(Magic).putInt(id).array())
          codec.encode(a, baos)
          baos.toByteArray
        }
      } yield arr
    }
  }
}
