package tamer.kafka

import tamer.AvroCodec
import zio.Task

import java.util.UUID

object KafkaTestUtils {
  case class Key(key: Int)
  object Key {
    implicit val codec = AvroCodec.codec[Key]
  }
  case class Value(value: Int)
  object Value {
    implicit val codec = AvroCodec.codec[Value]
  }
  case class State(i: Int)
  object State {
    implicit val codec = AvroCodec.codec[State]
  }

  def randomThing(prefix: String): Task[String] =
    Task(UUID.randomUUID()).map(uuid => s"$prefix-$uuid")
}
