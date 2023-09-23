package tamer

import zio._
import zio.kafka.serde.{Serde => ZSerde, Serializer}

import scala.annotation.implicitNotFound

sealed trait Serdes[K, V, SV] {
  def keySerializer: Serializer[Any, K]
  def valueSerializer: Serializer[Any, V]
  def stateKeySerde: ZSerde[Any, Tamer.StateKey]
  def stateValueSerde: ZSerde[Any, SV]
}

@implicitNotFound(
  "\n" +
    "Could not find or construct a \u001b[36mtamer.SerdesProvider\u001b[0m instance for types:\n" +
    "\n" +
    "  \u001b[32m${K}\u001b[0m, \u001b[32m${V}\u001b[0m, \u001b[32m${SV}\u001b[0m\n" +
    "\n" +
    "This can happen for a few reasons, but the most common case is a(/some) missing implicit(/implicits).\n" +
    "\n" +
    "Specifically, you need to ensure that wherever you are expected to provide a\n" +
    "\n" +
    "  \u001b[36mtamer.SchemaProvider[\u001b[32m${K}\u001b[0m, \u001b[32m${V}\u001b[0m, \u001b[32m${SV}\u001b[0m\u001b[36m]\u001b[0m\n" +
    "\n" +
    "All the following implicits are available:\n" +
    "  - \u001b[36mtamer.Codec[\u001b[32m${K}\u001b[0m\u001b[36m]{type \u001b[32mS\u001b[0m = <some type of schema>\u001b[36m}\u001b[0m\n" +
    "  - \u001b[36mtamer.Codec[\u001b[32m${V}\u001b[0m\u001b[36m]{type \u001b[32mS\u001b[0m = <some type of schema>\u001b[36m}\u001b[0m\n" +
    "  - \u001b[36mtamer.Codec[\u001b[32mtamer.Tamer.StateKey\u001b[0m\u001b[36m]{type \u001b[32mS\u001b[0m = <some type of schema>\u001b[36m}\u001b[0m\n" +
    "  - \u001b[36mtamer.Codec[\u001b[32m${SV}\u001b[0m\u001b[36m]{type \u001b[32mS\u001b[0m = <some type of schema>\u001b[36m}\u001b[0m\n" +
    "  (note that all \u001b[32mS\u001b[0m MUST be the same)\n" +
    "  - \u001b[36mtamer.SchemaParser[\u001b[32mS\u001b[0m, \u001b[32mPS\u001b[0m\u001b[36m]\u001b[0m (for the same \u001b[32mS\u001b[0m above)\n" +
    "  - \u001b[36mtamer.RegistryProvider[\u001b[32mPS\u001b[0m\u001b[36m]\u001b[0m (for the same \u001b[32mPS\u001b[0m above)\n"
)
sealed trait SerdesProvider[K, V, SV] {
  def using(maybeRegistryConfig: Option[RegistryConfig]): ZIO[Scope, TamerError, Serdes[K, V, SV]]
}

object SerdesProvider {
  implicit final def serdesProviderFromCodecs[K, V, SV, S, PS](
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
}
