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

trait State {

  /** Used for hashing purposes1
    */
  val stateId: Int
}

final case class ResultMetadata(queryExecutionTime: Long)
final case class QueryResult[V](metadata: ResultMetadata, results: List[V])

case class Setup[
    K <: Product: Encoder: Decoder: SchemaFor,
    V <: Product: Encoder: Decoder: SchemaFor,
    S <: Product with State: Encoder: Decoder: SchemaFor
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
      queryBuilder.queryId + defaultState.stateId
    )
