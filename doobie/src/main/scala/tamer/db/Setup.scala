package tamer
package db

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import doobie.Query0
import zio.UIO

import java.time.{Duration, Instant}

trait QueryBuilder[V, -S] {

  /** Used for hashing purposes
    */
  val queryId: Int
  def query(state: S): Query0[V]
}

final case class ResultMetadata(queryExecutionTime: Long)
final case class QueryResult[V](metadata: ResultMetadata, results: List[V])

final case class Setup[
    K <: Product: Encoder: Decoder: SchemaFor,
    V <: Product: Encoder: Decoder: SchemaFor,
    S <: Product with HashableState: Encoder: Decoder: SchemaFor
](
    queryBuilder: QueryBuilder[V, S],
    override val defaultState: S,
    keyExtract: V => K,
    stateFoldM: S => QueryResult[V] => UIO[S]
) extends _root_.tamer.Setup[K, V, S](
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
object Setup {
  final def apply[
      K <: Product: Encoder: Decoder: SchemaFor,
      V <: Product: Encoder: Decoder: SchemaFor,
      S <: Product with HashableState: Encoder: Decoder: SchemaFor
  ](
      queryBuilder: S => Query0[V]
  )(defaultState: S, keyExtract: V => K, stateFoldM: S => QueryResult[V] => UIO[S]): Setup[K, V, S] = {
    val qBuilder = new QueryBuilder[V, S] {
      override val queryId: Int               = queryBuilder(defaultState).sql.hashCode
      override def query(state: S): Query0[V] = queryBuilder(state)
    }
    new Setup[K, V, S](queryBuilder = qBuilder, defaultState = defaultState, keyExtract = keyExtract, stateFoldM = stateFoldM)
  }

  final def fromTimeSegment[K <: Product: Encoder: Decoder: SchemaFor, V <: Product with Timestamped: Ordering: Encoder: Decoder: SchemaFor](
      queryBuilder: TimeSegment => Query0[V]
  )(earliest: Instant, tumblingStep: Duration, keyExtract: V => K): Setup[K, V, TimeSegment] = {

    val timeSegment = TimeSegment(earliest, earliest.plus(tumblingStep))

    def stateFold(timeSegment: TimeSegment)(queryResult: QueryResult[V]): UIO[TimeSegment] =
      if (queryResult.results.isEmpty) timeSegment.to.plus(tumblingStep).orNow.map(TimeSegment(timeSegment.from, _))
      else {
        val mostRecent = queryResult.results.max.timestamp
        mostRecent.plus(tumblingStep).orNow.map(TimeSegment(mostRecent, _))
      }

    Setup(queryBuilder)(timeSegment, keyExtract, stateFold)
  }
}
