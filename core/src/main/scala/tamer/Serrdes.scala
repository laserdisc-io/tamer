package tamer

import zio._
import zio.kafka.serde.{Serde => ZSerde, Serializer}

sealed trait Serdes[K, V, SV] {
  def keySerializer: Serializer[Any, K]
  def valueSerializer: Serializer[Any, V]
  def stateKeySerde: ZSerde[Any, Tamer.StateKey]
  def stateValueSerde: ZSerde[Any, SV]
}

sealed trait SerdesProvider[K, V, SV] {
  def using(maybeRegistryConfig: Option[RegistryConfig]): ZIO[Scope, TamerError, Serdes[K, V, SV]]
}

object SerdesProvider {
  sealed abstract case class SerdesProviderImpl[K, V, SV, PS](
      keySerde: Serde[PS, K],
      valueSerde: Serde[PS, V],
      stateKeySerde: Serde[PS, Tamer.StateKey],
      stateValueSerde: Serde[PS, SV],
      registryProvider: RegistryProvider[PS]
  ) extends SerdesProvider[K, V, SV] { self =>
    override final def using(maybeRegistryConfig: Option[RegistryConfig]): ZIO[Scope, TamerError, Serdes[K, V, SV]] =
      maybeRegistryConfig.fold(Registry.fakeRegistryZIO[PS])(registryProvider.from(_)).map { registry =>
        new Serdes[K, V, SV] {
          override final val keySerializer: Serializer[Any, K]          = self.keySerde.erase(registry)
          override final val valueSerializer: Serializer[Any, V]        = self.valueSerde.erase(registry)
          override final val stateKeySerde: ZSerde[Any, Tamer.StateKey] = self.stateKeySerde.erase(registry)
          override final val stateValueSerde: ZSerde[Any, SV]           = self.stateValueSerde.erase(registry)
        }
      }
  }

  implicit final def SerdesProviderFromCodecs[K, V, SV, S, PS](
      implicit K: Codec.Aux[K, S],
      V: Codec.Aux[V, S],
      SK: Codec.Aux[Tamer.StateKey, S],
      SV: Codec.Aux[SV, S],
      PS: SchemaParser[S, PS],
      registryProvider: RegistryProvider[PS],
      ev: Tag[PS]
  ): SerdesProvider[K, V, SV] = new SerdesProvider[K, V, SV] {
    override final def using(maybeRegistryConfig: Option[RegistryConfig]): ZIO[Scope, TamerError, Serdes[K, V, SV]] =
      maybeRegistryConfig.fold(Registry.fakeRegistryZIO[PS])(registryProvider.from(_)).map { registry =>
        new Serdes[K, V, SV] {
          override final val keySerializer: Serializer[Any, K]          = Serde.key(K, PS).erase(registry)
          override final val valueSerializer: Serializer[Any, V]        = Serde.value(V, PS).erase(registry)
          override final val stateKeySerde: ZSerde[Any, Tamer.StateKey] = Serde.key(SK, PS).erase(registry)
          override final val stateValueSerde: ZSerde[Any, SV]           = Serde.value(SV, PS).erase(registry)
        }
      }
  }
  // new SerdesProviderImpl(Serde.key(K, PS), Serde.value(V, PS), Serde.key(SK, PS), Serde.value(SV, PS), registryProvider) {}
}
