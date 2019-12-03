package tamer

import java.io.ByteArrayOutputStream
import java.util.UUID

import com.sksamuel.avro4s._
import org.apache.avro.Schema
import zio.RIO
import zio.kafka.client.serde._

import scala.annotation.implicitNotFound

object Serdes {
  final def apply[A <: Product: SchemaFor] = new SerdesPartiallyApplied(SchemaFor[A])

  final class SerdesPartiallyApplied[A](private val schemaForA: SchemaFor[A]) extends AnyVal {
    private def schema = schemaForA.schema(DefaultFieldMapper)
    private def deserWith(schema: Schema)(implicit `_`: Decoder[A]): Deserializer[Any, A] = {
      def deserialize(ba: Array[Byte]) = AvroInputStream.binary[A].from(ba).build(schema)
      Deserializer.byteArray.mapM { ba =>
        RIO.fromTry(deserialize(ba).tryIterator.next)
      }
    }
    private def serWith(schema: Schema)(implicit `_`: Encoder[A]): Serializer[Any, A] = {
      def serialize(baos: ByteArrayOutputStream) = AvroOutputStream.binary[A].to(baos).build(schema)
      Serializer.byteArray.contramapM { a =>
        RIO {
          val baos = new ByteArrayOutputStream
          val ser  = serialize(baos)
          ser.write(a)
          ser.close()
          baos.toByteArray
        }
      }
    }

    final def deserializer(implicit `_`: Decoder[A]): Deserializer[Any, A] = deserWith(schema)
    final def serializer(implicit `_`: Encoder[A]): Serializer[Any, A]     = serWith(schema)
    final def serde(implicit `_`: Decoder[A], `__`: Encoder[A]): Serde[Any, A] = {
      val s = schema
      Serde(deserWith(s))(serWith(s))
    }
  }

  @implicitNotFound("Couldn't find Simple[$A] instance, try implementing your own")
  sealed trait Simple[A] extends Any {
    def deserializer: Deserializer[Any, A]
    def serializer: Serializer[Any, A]
    final def serde: Serde[Any, A] = Serde(deserializer)(serializer)
  }

  object Simple {
    @inline final def apply[A](implicit S: Simple[A]): Simple[A] = S

    implicit final val boolSerde: Simple[Boolean] = new Simple[Boolean] {
      override final val deserializer: Deserializer[Any, Boolean] = Serde.short.map(_ == 0)
      override final val serializer: Serializer[Any, Boolean]     = Serde.short.contramap(b => if (b) 1 else 0)
    }
    implicit final val doubleSerde: Simple[Double] = new Simple[Double] {
      override final val deserializer: Deserializer[Any, Double] = Serde.double
      override final val serializer: Serializer[Any, Double]     = Serde.double
    }
    implicit final val floatSerde: Simple[Float] = new Simple[Float] {
      override final val deserializer: Deserializer[Any, Float] = Serde.float
      override final val serializer: Serializer[Any, Float]     = Serde.float
    }
    implicit final val intSerde: Simple[Int] = new Simple[Int] {
      override final val deserializer: Deserializer[Any, Int] = Serde.int
      override final val serializer: Serializer[Any, Int]     = Serde.int
    }
    implicit final val longSerde: Simple[Long] = new Simple[Long] {
      override final val deserializer: Deserializer[Any, Long] = Serde.long
      override final val serializer: Serializer[Any, Long]     = Serde.long
    }
    implicit final val stringSerde: Simple[String] = new Simple[String] {
      override final val deserializer: Deserializer[Any, String] = Serde.string
      override final val serializer: Serializer[Any, String]     = Serde.string
    }
    implicit final val uiidSerde: Simple[UUID] = new Simple[UUID] {
      override final val deserializer: Deserializer[Any, UUID] = Serde.uuid
      override final val serializer: Serializer[Any, UUID]     = Serde.uuid
    }
  }
}
