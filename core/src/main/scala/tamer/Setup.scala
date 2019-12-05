package tamer

import com.sksamuel.avro4s._
import doobie.util.query.Query0
import zio.UIO
import zio.kafka.client.serde._

final case class Setup[K, V, State](
    keySerializer: Serializer[Any, K],
    valueSerializer: Serializer[Any, V],
    stateSerde: Serde[Any, State],
    valueToKey: V => K,
    defaultState: State,
    buildQuery: State => Query0[V],
    stateFoldM: State => List[V] => UIO[State]
)

object Setup {
  final def avro[K <: Product: Encoder: SchemaFor, V <: Product: Encoder: SchemaFor, State <: Product: Decoder: Encoder: SchemaFor](
      defaultState: State
  )(
      buildQuery: State => Query0[V]
  )(
      valueToKey: V => K,
      stateFoldM: State => List[V] => UIO[State]
  ): Setup[K, V, State] = Setup(Serdes[K].serializer, Serdes[V].serializer, Serdes[State].serde, valueToKey, defaultState, buildQuery, stateFoldM)
  final def avroSimple[K <: Product: Encoder: SchemaFor, V <: Product: Decoder: Encoder: SchemaFor](
      defaultState: V
  )(
      buildQuery: V => Query0[V],
      valueToKey: V => K
  ): Setup[K, V, V] = Setup(Serdes[K].serializer, Serdes[V].serializer, Serdes[V].serde, valueToKey, defaultState, buildQuery, _ => l => UIO(l.last))

  final def avroK[K: Serdes.Simple, V <: Product: Encoder: SchemaFor, State <: Product: Decoder: Encoder: SchemaFor](
      defaultState: State
  )(
      buildQuery: State => Query0[V]
  )(
      valueToKey: V => K,
      stateFoldM: State => List[V] => UIO[State]
  ): Setup[K, V, State] =
    Setup(Serdes.Simple[K].serializer, Serdes[V].serializer, Serdes[State].serde, valueToKey, defaultState, buildQuery, stateFoldM)
  final def avroSimpleK[K: Serdes.Simple, V <: Product: Decoder: Encoder: SchemaFor](
      defaultState: V
  )(
      buildQuery: V => Query0[V],
      valueToKey: V => K
  ): Setup[K, V, V] =
    Setup(Serdes.Simple[K].serializer, Serdes[V].serializer, Serdes[V].serde, valueToKey, defaultState, buildQuery, _ => l => UIO(l.last))
}
