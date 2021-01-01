package tamer

import tamer.registry.{Registry, Topic}
import zio.kafka.serde.Serializer

abstract class Setup[-K, -V, S](
    val keySerializer: Serializer[Registry with Topic, K],
    val valueSerializer: Serializer[Registry with Topic, V],
    val stateSerde: ZSerde[Registry with Topic, S],
    val defaultState: S,
    val stateKey: Int
) {
  def show: String = "not available, please implement the show method to display setup"
}
