/*
 * Copyright (c) 2019-2025 LaserDisc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

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
    "  \u001b[36mtamer.SerdesProvider[\u001b[32m${K}\u001b[0m, \u001b[32m${V}\u001b[0m, \u001b[32m${SV}\u001b[0m\u001b[36m]\u001b[0m\n" +
    "\n" +
    "All the following implicits are available:\n" +
    "  - \u001b[36mtamer.Codec[\u001b[32m${K}\u001b[0m\u001b[36m]\u001b[0m\n" +
    "  - \u001b[36mtamer.Codec[\u001b[32m${V}\u001b[0m\u001b[36m]\u001b[0m\n" +
    "  - \u001b[36mtamer.Codec[\u001b[32mtamer.Tamer.StateKey\u001b[0m\u001b[36m]\u001b[0m\n" +
    "  - \u001b[36mtamer.Codec[\u001b[32m${SV}\u001b[0m\u001b[36m]\u001b[0m\n" +
    "  - \u001b[36mtamer.RegistryProvider\u001b[0m\n"
)
sealed trait SerdesProvider[K, V, SV] {
  def using(maybeRegistryConfig: Option[RegistryConfig]): RIO[Scope, Serdes[K, V, SV]]
}

object SerdesProvider {
  implicit final def serdesProviderFromCodecs[K: Codec, V: Codec, SV: Codec](
      implicit SK: Codec[Tamer.StateKey],
      registryProvider: RegistryProvider
  ): SerdesProvider[K, V, SV] = new SerdesProvider[K, V, SV] {
    override final def using(maybeRegistryConfig: Option[RegistryConfig]): RIO[Scope, Serdes[K, V, SV]] =
      maybeRegistryConfig.fold(Registry.fakeRegistryZIO)(registryProvider.from(_)).map { registry =>
        new Serdes[K, V, SV] {
          override final val keySerializer: Serializer[Any, K]          = Serde.key[K].using(registry)
          override final val valueSerializer: Serializer[Any, V]        = Serde.value[V].using(registry)
          override final val stateKeySerde: ZSerde[Any, Tamer.StateKey] = Serde.key[Tamer.StateKey].using(registry)
          override final val stateValueSerde: ZSerde[Any, SV]           = Serde.value[SV].using(registry)
        }
      }
  }
}
