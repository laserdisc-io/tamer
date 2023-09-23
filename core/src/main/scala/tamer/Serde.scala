package tamer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import org.apache.kafka.common.header.Headers
import zio._
import zio.kafka.serde.{Serde => ZSerde}

sealed abstract case class Serde[PS: Tag, A](isKey: Boolean, codec: Codec[A], maybeResolvedSchema: Option[PS]) extends ZSerde[Registry[PS], A] {
  self =>
  final val Magic: Byte = 0x0
  final val IntByteSize = 4

  final def deserialize(data: Array[Byte]): Task[A] = ZIO.attempt(codec.decode(new ByteArrayInputStream(data)))
  final def serialize(value: A): Task[Array[Byte]] = ZIO.attempt {
    val baos = new ByteArrayOutputStream
    codec.encode(value, baos)
    baos.toByteArray
  }
  final def subject(topic: String): String = s"$topic-${if (isKey) "key" else "value"}"

  override def deserialize(topic: String, headers: Headers, data: Array[Byte]): RIO[Registry[PS], A] = ZIO.serviceWithZIO {
    case Registry.FakeRegistry => deserialize(data)
    case registry =>
      maybeResolvedSchema.fold(deserialize(data)) { schema =>
        val buffer = ByteBuffer.wrap(data)
        if (buffer.get() != Magic) ZIO.fail(TamerError("Deserialization failed: unknown magic byte!"))
        else {
          val id = buffer.getInt()
          for {
            _ <- registry.verifySchema(id, schema)
            res <- ZIO.attempt {
              val length  = buffer.limit() - (IntByteSize + 1)
              val payload = new Array[Byte](length)
              buffer.get(payload, 0, length)
              codec.decode(new ByteArrayInputStream(payload))
            }
          } yield res
        }
      }
  }

  override def serialize(topic: String, headers: Headers, value: A): RIO[Registry[PS], Array[Byte]] = ZIO.serviceWithZIO {
    case Registry.FakeRegistry => serialize(value)
    case registry =>
      maybeResolvedSchema.fold(serialize(value)) { schema =>
        for {
          id <- registry.getOrRegisterId(subject(topic), schema)
          arr <- ZIO.attempt {
            val baos = new ByteArrayOutputStream
            baos.write(ByteBuffer.allocate(IntByteSize + 1).put(Magic).putInt(id).array())
            codec.encode(value, baos)
            baos.toByteArray
          }
        } yield arr
      }
  }

  final def erase(registry: Registry[PS]): ZSerde[Any, A] = new ZSerde[Any, A] {
    private final val layer = ZLayer.succeed(registry)
    override def deserialize(topic: String, headers: Headers, data: Array[Byte]): Task[A] =
      self.deserialize(topic, headers, data).provideLayer(layer)
    override def serialize(topic: String, headers: Headers, value: A): Task[Array[Byte]] =
      self.serialize(topic, headers, value).provideLayer(layer)
  }
}

object Serde {
  final def key[A, S, PS: Tag](codec: Codec.Aux[A, S], schemaParser: SchemaParser[S, PS]): Serde[PS, A] =
    new Serde(isKey = true, codec, codec.maybeSchema.flatMap(schemaParser.parse)) {}
  final def value[A, S, PS: Tag](codec: Codec.Aux[A, S], schemaParser: SchemaParser[S, PS]): Serde[PS, A] =
    new Serde(isKey = false, codec, codec.maybeSchema.flatMap(schemaParser.parse)) {}
}
