package tamer

import zio._
import zio.kafka.serde.{Serde => ZSerde, Serializer}

abstract class Setup[-R, K: Tag, V: Tag, S: Tag] {
  val serdes: Setup.Serdes[K, V, S]
  val initialState: S
  val stateKey: Int
  val recordKey: (S, V) => K
  val repr: String = "no repr string implemented, if you want a neat description of the source configuration please implement it"
  def iteration(currentState: S, queue: Enqueue[NonEmptyChunk[(K, V)]]): RIO[R, S]

  final val run: ZIO[R with KafkaConfig, TamerError, Unit] = runLoop.provideSomeLayer(Tamer.live(this))
  final def runWith[E >: TamerError, R1](layer: Layer[E, R with KafkaConfig with R1]): ZIO[Scope, E, Unit] =
    runLoop.provideLayer(layer >>> Tamer.live(this))
}

object Setup {
  sealed abstract class Serdes[-K, -V, S](
      val keySerializer: Serializer[Registry, K],
      val valueSerializer: Serializer[Registry, V],
      val stateKeySerde: ZSerde[Registry, Tamer.StateKey],
      val stateValueSerde: ZSerde[Registry, S]
  )

  def mkSerdes[K: Codec, V: Codec, S: Codec](implicit ev: Codec[Tamer.StateKey]): Serdes[K, V, S] =
    new Serdes(Serde.key[K], Serde.value[V], Serde.key[Tamer.StateKey], Serde.value[S]) {}
}
