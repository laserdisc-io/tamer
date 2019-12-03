package tamer

import com.sksamuel.avro4s._
import doobie.util.query.Query0
import zio.kafka.client.serde._

final case class Setup[K, V, State](
    keySerializer: Serializer[Any, K],
    valueSerializer: Serializer[Any, V],
    stateSerde: Serde[Any, State],
    valueToKey: V => K,
    defaultState: State,
    buildQuery: State => Query0[V],
    stateFold: State => List[V] => State
)

object Setup {
  final def avro[K <: Product: Encoder: SchemaFor, V <: Product: Encoder: SchemaFor, State <: Product: Decoder: Encoder: SchemaFor](
      defaultState: State
  )(
      buildQuery: State => Query0[V]
  )(
      valueToKey: V => K,
      stateFold: State => List[V] => State
  ): Setup[K, V, State] = Setup(Serdes[K].serializer, Serdes[V].serializer, Serdes[State].serde, valueToKey, defaultState, buildQuery, stateFold)
  final def avroSimple[K <: Product: Encoder: SchemaFor, V <: Product: Decoder: Encoder: SchemaFor](
      defaultState: V
  )(
      buildQuery: V => Query0[V],
      valueToKey: V => K
  ): Setup[K, V, V] = Setup(Serdes[K].serializer, Serdes[V].serializer, Serdes[V].serde, valueToKey, defaultState, buildQuery, _ => _.last)

  final def avroK[K: Serdes.Simple, V <: Product: Encoder: SchemaFor, State <: Product: Decoder: Encoder: SchemaFor](
      defaultState: State
  )(
      buildQuery: State => Query0[V]
  )(
      valueToKey: V => K,
      stateFold: State => List[V] => State
  ): Setup[K, V, State] =
    Setup(Serdes.Simple[K].serializer, Serdes[V].serializer, Serdes[State].serde, valueToKey, defaultState, buildQuery, stateFold)
  final def avroSimpleK[K: Serdes.Simple, V <: Product: Decoder: Encoder: SchemaFor](
      defaultState: V
  )(
      buildQuery: V => Query0[V],
      valueToKey: V => K
  ): Setup[K, V, V] = Setup(Serdes.Simple[K].serializer, Serdes[V].serializer, Serdes[V].serde, valueToKey, defaultState, buildQuery, _ => _.last)
}
