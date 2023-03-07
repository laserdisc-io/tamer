package tamer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import org.apache.kafka.common.header.Headers
import zio.{RIO, ZIO}
import zio.kafka.serde.{Serde => ZSerde}

object Serde {
  final def key[A: Codec]   = impl(isKey = true, Codec[A])
  final def value[A: Codec] = impl(isKey = false, Codec[A])

  private[this] def impl[A](isKey: Boolean, codec: Codec[A]): ZSerde[Registry, A] = {
    val Magic: Byte = 0x0
    val IntByteSize = 4

    def deserializeSimple(data: Array[Byte]) = ZIO.attempt(codec.decode(new ByteArrayInputStream(data)))
    def serializeSimple(value: A) = ZIO.attempt {
      val baos = new ByteArrayOutputStream
      codec.encode(value, baos)
      baos.toByteArray
    }
    def subject(topic: String): String = s"$topic-${if (isKey) "key" else "value"}"

    new ZSerde[Registry, A] {
      override def deserialize(topic: String, headers: Headers, data: Array[Byte]): RIO[Registry, A] = ZIO.serviceWithZIO {
        case Registry.Fake => deserializeSimple(data)
        case registry =>
          codec.maybeSchema.fold(deserializeSimple(data)) { schema =>
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

      override def serialize(topic: String, headers: Headers, value: A): RIO[Registry, Array[Byte]] = ZIO.serviceWithZIO {
        case Registry.Fake => serializeSimple(value)
        case registry =>
          codec.maybeSchema.fold(serializeSimple(value)) { schema =>
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
    }
  }
}
