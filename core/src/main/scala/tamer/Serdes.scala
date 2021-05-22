package tamer

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import tamer.registry._
import zio.kafka.serde.{Deserializer, Serializer}
import zio.{RIO, Task}

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

object AvroCodec {
  def codec[V](implicit e: Encoder[V], d: Decoder[V], s: SchemaFor[V]): Codec[V] = new Codec[V] {
    override def decode(value: Any): V = d.decode(value)

    override def encode(value: V): AnyRef = e.encode(value)

    override def schemaFor: SchemaFor[V] = s
  }
}

sealed trait Serde[A] extends Any {
  def isKey: Boolean

  def schema: Schema

  def deserializer: Deserializer[RegistryInfo, A]

  def serializer: Serializer[RegistryInfo, A]

  final def serde: ZSerde[RegistryInfo, A] = ZSerde(deserializer)(serializer)
}

object Serde {
  private[this] final val Magic: Byte = 0x0
  private[this] final val intByteSize = 4

  final def apply[A: Codec](isKey: Boolean = false) =
    new RecordSerde[A](isKey, implicitly[Codec[A]].codec.schema)

  final class RecordSerde[A: Decoder: Encoder](override final val isKey: Boolean, override final val schema: Schema) extends Serde[A] {
    private[this] def subject(topic: String): String = s"$topic-${if (isKey) "key" else "value"}"

    override final val deserializer: Deserializer[RegistryInfo, A] = Deserializer.byteArray.mapM { ba =>
      val buffer = ByteBuffer.wrap(ba)
      if (buffer.get() != Magic) RIO.fail(TamerError("Deserialization failed: unknown magic byte!"))
      else {
        val id = buffer.getInt()
        for {
          _ <- registry.verifySchema(id, schema)
          res <- RIO.fromTry {
            val length  = buffer.limit() - 1 - intByteSize
            val payload = new Array[Byte](length)
            buffer.get(payload, 0, length)
            AvroInputStream.binary[A].from(payload).build(schema).tryIterator.next()
          }
        } yield res
      }
    }
    override final val serializer: Serializer[RegistryInfo, A] = Serializer.byteArray.contramapM { a =>
      for {
        t  <- registry.topic
        id <- registry.getOrRegisterId(subject(t), schema)
        arr <- Task {
          val baos = new ByteArrayOutputStream
          baos.write(Magic.toInt)
          baos.write(ByteBuffer.allocate(intByteSize).putInt(id).array())
          val ser = AvroOutputStream.binary[A].to(baos).build()
          ser.write(a)
          ser.close()
          baos.toByteArray
        }
      } yield arr
    }
  }

}
