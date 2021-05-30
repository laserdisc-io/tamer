package tamer

import zio.Task

import java.util.UUID

object TestUtils {
  case class Key(key: Int)
  case class Value(value: Int)
  case class State(state: Int)

  def randomThing(prefix: String): Task[String] =
    Task(UUID.randomUUID()).map(uuid => s"$prefix-$uuid")
}
