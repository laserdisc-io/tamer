package tamer

import zio._
import zio.kafka.serde.{Serde => ZSerde, Serializer}

abstract class Setup[-R, K: Tag, V: Tag, SV: Tag, RS: Tag] {
  val serdes: Setup.Serdes[K, V, SV, RS]
  val initialState: SV
  val stateKey: Int
  val recordKey: (SV, V) => K
  val repr: String = "no repr string implemented, if you want a neat description of the source configuration please implement it"
  def iteration(currentState: SV, queue: Enqueue[NonEmptyChunk[(K, V)]]): RIO[R, SV]

  final val run: ZIO[R with KafkaConfig, TamerError, Unit] = runLoop.provideSomeLayer(Tamer.live(this))
  final def runWith[E >: TamerError, R1](layer: Layer[E, R with KafkaConfig with R1]): IO[E, Unit] =
    runLoop.provideLayer(layer >>> Tamer.live(this))
}

object Setup {
  sealed abstract class Serdes[-K, -V, -SV, RS](
      val keySerializer: Serializer[Registry[RS], K],
      val valueSerializer: Serializer[Registry[RS], V],
      val stateKeySerde: ZSerde[Registry[RS], Tamer.StateKey],
      val stateValueSerde: ZSerde[Registry[RS], SV]
  )

  def apply[K, V, SV] = new {
    def mkSerdes[S: Codec.Aux[K, *]: Codec.Aux[V, *]: Codec.Aux[Tamer.StateKey, *]: Codec.Aux[SV, *], RS: Tag: SchemaResolver[S, *]]: Serdes[K, V, SV, RS] =
      new Serdes(Serde[K].key, Serde[V].value, Serde[Tamer.StateKey].key, Serde[SV].value) {}
  }
}
