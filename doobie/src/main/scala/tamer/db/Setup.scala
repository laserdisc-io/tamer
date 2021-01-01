package tamer.db

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import doobie.Query0
import tamer.Serde
import zio.UIO

trait QueryBuilder[V, -S] {

  /** Used for hashing purposes
    */
  val queryId: Int
  def query(state: S): Query0[V]
}

trait HashableState {

  /**  It is required for this hash to be consistent even across executions
    *  for the same semantic state. This is in contrast with the built-in
    *  `hashCode` method.
    */
  val stateHash: Int
}

final case class ResultMetadata(queryExecutionTime: Long)
final case class QueryResult[V](metadata: ResultMetadata, results: List[V])

case class Setup[
    K <: Product: Encoder: Decoder: SchemaFor,
    V <: Product: Encoder: Decoder: SchemaFor,
    S <: Product with HashableState: Encoder: Decoder: SchemaFor
](
    queryBuilder: QueryBuilder[V, S],
    override val defaultState: S,
    keyExtract: V => K,
    stateFoldM: S => QueryResult[V] => UIO[S]
) extends tamer.Setup[K, V, S](
      Serde[K](isKey = true).serializer,
      Serde[V]().serializer,
      Serde[S]().serde,
      defaultState,
      queryBuilder.queryId + defaultState.stateHash
    ) {
  override def show: String = s"""
      |query:             ${queryBuilder.query(defaultState).sql}
      |query id:          ${queryBuilder.queryId}
      |default state:     $defaultState
      |default state id:  ${defaultState.stateHash}
      |default state key: $stateKey
      |""".stripMargin.stripLeading()
}
