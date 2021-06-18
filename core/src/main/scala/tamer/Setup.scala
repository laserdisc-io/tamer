package tamer

import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.kafka.serde.Serializer

abstract class Setup[-R, K, V, S] {
  val serdes: Setup.Serdes[K, V, S]
  val initialState: S
  val stateKey: Int
  val recordKey: (S, V) => K
  val repr: String = "no repr string implemented, if you want a neat description of the source configuration please implement it"
  def iteration(currentState: S, queue: Queue[Chunk[(K, V)]]): RIO[R, S]

  final val run: ZIO[R with Has[KafkaConfig] with Blocking with Clock, TamerError, Unit] = runLoop.provideSomeLayer(Tamer.live(this))
  final def runWith[E >: TamerError, R1 <: Has[_]](layer: ZLayer[ZEnv, E, R1])(
      implicit ev: ZEnv with R1 <:< R with Has[KafkaConfig] with Blocking with Clock,
      tagged: Tag[R1]
  ): ZIO[ZEnv, E, Unit] = run.provideCustomLayer(layer)
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
