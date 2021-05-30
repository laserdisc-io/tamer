package tamer

import zio.kafka.serde.Serializer

case class Setup[-K, -V, S](
    serdes: Setup.Serdes[K, V, S],
    defaultState: S,
    tamerStateKafkaRecordKey: Int,
    repr: String = "no repr string implemented, if you want a neat description of the source configuration please implement it"
)

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
