package tamer

import com.sksamuel.avro4s._
import doobie.util.query.Query0
import tamer.registry.{Registry, Topic}
import zio.UIO
import zio.kafka.serde.Serializer

final case class ResultMetadata(queryExecutionTime: Long)
final case class QueryResult[V](metadata: ResultMetadata, results: List[V])

final case class Setup[K, V, State](
    keySerializer: Serializer[Registry with Topic, K],
    valueSerializer: Serializer[Registry with Topic, V],
    stateSerde: ZSerde[Registry with Topic, State],
    valueToKey: V => K,
    defaultState: State,
    buildQuery: State => Query0[V],
    stateFoldM: State => QueryResult[V] => UIO[State]
)

object Setup {
  final def avro[K <: Product: Decoder: Encoder: SchemaFor, V <: Product: Decoder: Encoder: SchemaFor, State <: Product: Decoder: Encoder: SchemaFor](
      defaultState: State
  )(
      buildQuery: State => Query0[V]
  )(
      valueToKey: V => K,
      stateFoldM: State => QueryResult[V] => UIO[State]
  ): Setup[K, V, State] =
    Setup(Serde[K](isKey = true).serializer, Serde[V]().serializer, Serde[State]().serde, valueToKey, defaultState, buildQuery, stateFoldM)

  final def avroSimple[K <: Product: Decoder: Encoder: SchemaFor, V <: Product: Decoder: Encoder: SchemaFor](
      defaultState: V
  )(
      buildQuery: V => Query0[V],
      valueToKey: V => K
  ): Setup[K, V, V] =
    Setup(
      Serde[K](isKey = true).serializer,
      Serde[V]().serializer,
      Serde[V]().serde,
      valueToKey,
      defaultState,
      buildQuery,
      _ => r => UIO(r.results.last)
    )
}
