package tamer

import zio._
import zio.kafka.serde.{Serde => ZSerde, Serializer}

abstract class Setup[-R, K: Tag, V: Tag, SV: Tag] {
  val mkSerdes: Setup.MkSerdes[K, V, SV]
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
  sealed trait Serdes[K, V, SV] {
    def keySerializer: Serializer[Any, K]
    def valueSerializer: Serializer[Any, V]
    def stateKeySerde: ZSerde[Any, Tamer.StateKey]
    def stateValueSerde: ZSerde[Any, SV]
  }
  sealed trait MkSerdes[K, V, SV] {
    def using(maybeRegistryConfig: Option[RegistryConfig]): ZIO[Scope, TamerError, Serdes[K, V, SV]]
  }
  sealed abstract case class MkSerdesImpl[K, V, SV, RS](
      keySerde: Serde[RS, K],
      valueSerde: Serde[RS, V],
      stateKeySerde: Serde[RS, Tamer.StateKey],
      stateValueSerde: Serde[RS, SV],
      registryProvider: RegistryProvider[RS]
  ) extends MkSerdes[K, V, SV] { self =>
    override final def using(maybeRegistryConfig: Option[RegistryConfig]): ZIO[Scope, TamerError, Serdes[K, V, SV]] = {
      val registryZIO = maybeRegistryConfig.fold(Registry.fakeRegistryZIO[RS])(registryProvider.from(_))
      registryZIO.map { registry =>
        val registryLayer = ZLayer.succeed(registry)
        new Serdes[K, V, SV] {
          override final val keySerializer: Serializer[Any, K]          = self.keySerde.eraseLayer(registryLayer)
          override final val valueSerializer: Serializer[Any, V]        = self.valueSerde.eraseLayer(registryLayer)
          override final val stateKeySerde: ZSerde[Any, Tamer.StateKey] = self.stateKeySerde.eraseLayer(registryLayer)
          override final val stateValueSerde: ZSerde[Any, SV]           = self.stateValueSerde.eraseLayer(registryLayer)
        }
      }
    }
  }

  sealed abstract class MkSerdesPartiallyApplied[K, V, SV] {
    def make[S, RS](
        implicit K: Codec.Aux[K, S],
        V: Codec.Aux[V, S],
        SK: Codec.Aux[Tamer.StateKey, S],
        SV: Codec.Aux[SV, S],
        RS: SchemaResolver[S, RS],
        registryProvider: RegistryProvider[RS],
        ev: Tag[RS]
    ): MkSerdes[K, V, SV] = new MkSerdesImpl(Serde.key(K, RS), Serde.value(V, RS), Serde.key(SK, RS), Serde.value(SV, RS), registryProvider) {}
  }

  def forTypes[K, V, SV] = new MkSerdesPartiallyApplied[K, V, SV] {}
}
