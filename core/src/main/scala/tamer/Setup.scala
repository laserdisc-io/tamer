package tamer

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import tamer.registry.{Registry, Topic}
import zio.kafka.serde.Serializer

trait HashableState[S] {

  /**  It is required for this hash to be consistent even across executions
    *  for the same semantic state. This is in contrast with the built-in
    *  `hashCode` method.
    */
  def stateHash(s: S): Int
}

object HashableState {
  def apply[S](implicit hs: HashableState[S]): HashableState[S] = hs
}

abstract class Setup[-K, -V, S](
    val serde: Setup.SourceSerde[K, V, S],
    val defaultState: S,
    val tamerStateKafkaRecordKey: Int
) {
  def show: String = "not available, please implement the show method to display setup"
}

object Setup {
  case class SourceSerde[-K, -V, S](
                          keySerializer: Serializer[Registry with Topic, K],
                          valueSerializer: Serializer[Registry with Topic, V],
                          stateSerde: ZSerde[Registry with Topic, S],
                  )

  object SourceSerde {
    def apply[
      K <: Product : Encoder : Decoder : SchemaFor,
      V <: Product : Encoder : Decoder : SchemaFor,
      S <: Product : Decoder : Encoder : SchemaFor
    ](): SourceSerde[K, V, S] = SourceSerde(
      Serde[K](isKey = true).serializer,
      Serde[V]().serializer,
      Serde[S]().serde,
    )
  }
}