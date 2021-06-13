package tamer

import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.kafka.serde.Serializer

abstract class Setup[-R, K, V, S] {
  val serdes: Setup.Serdes[K, V, S]
  val defaultState: S
  val stateKey: Int
  val recordKey: (S, V) => K
  val repr: String = "no repr string implemented, if you want a neat description of the source configuration please implement it"
  def iteration(currentState: S, queue: Queue[Chunk[(K, V)]]): RIO[R, S]

  final def runLive: ZIO[R with Has[KafkaConfig] with Blocking with Clock, TamerError, Unit] = runLoop.provideSomeLayer(Tamer.live(this))
}

object Setup {
  sealed abstract class Serdes[-K, -V, S](
      val keySerializer: Serializer[RegistryInfo, K],
      val valueSerializer: Serializer[RegistryInfo, V],
      val stateSerde: ZSerde[RegistryInfo, S]
  )

  object Serdes {
    def apply[K: Codec, V: Codec, S: Codec]: Serdes[K, V, S] =
      new Serdes(Serde.key[K].serializer, Serde.value[V].serializer, Serde.value[S].serde) {}
  }

}
