package tamer

import com.sksamuel.avro4s.Codec
import tamer.registry.{Registry, Topic}
import zio.kafka.serde.Serializer

trait HashableState[S] {

  /** It is required for this hash to be consistent even across executions
    * for the same semantic state. This is in contrast with the built-in
    * `hashCode` method.
    */
  def stateHash(s: S): Int
}

object HashableState {
  def apply[S](implicit hs: HashableState[S]): HashableState[S] = hs
}

case class SourceConfiguration[-K, -V, S](
    serde: SourceConfiguration.SourceSerde[K, V, S],
    defaultState: S,
    tamerStateKafkaRecordKey: Int,
    repr: String
)

object SourceConfiguration {

  case class SourceSerde[-K, -V, S](
      keySerializer: Serializer[Registry with Topic, K],
      valueSerializer: Serializer[Registry with Topic, V],
      stateSerde: ZSerde[Registry with Topic, S]
  )

  object SourceSerde {
    def apply[
        K <: Product: Codec,
        V <: Product: Codec,
        S <: Product: Codec
    ](): SourceSerde[K, V, S] =
      SourceSerde(
        Serde[K](isKey = true).serializer,
        Serde[V]().serializer,
        Serde[S]().serde
      )
  }

}
