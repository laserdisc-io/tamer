package tamer
package db

import com.sksamuel.avro4s.Codec
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

final case class DoobieConfiguration[
  K <: Product : Codec,
  V <: Product : Codec,
  S <: Product : Codec : HashableState,
](
   queryBuilder: QueryBuilder[V, S],
   defaultState: S,
   keyExtract: V => K,
   stateFoldM: S => QueryResult[V] => UIO[S]
 ) {
  private val keyId = queryBuilder.queryId + HashableState[S].stateHash(defaultState)
  private val repr: String =
    s"""
       |query:             ${queryBuilder.query(defaultState).sql}
       |query id:          ${queryBuilder.queryId}
       |default state:     $defaultState
       |default state id:  ${HashableState[S].stateHash(defaultState)}
       |default state key: $keyId
       |""".stripMargin.stripLeading()

  val generic: SourceConfiguration[K, V, S] = SourceConfiguration(
    SourceConfiguration.SourceSerde[K, V, S](),
    defaultState,
    keyId,
    repr
  )

}

object DoobieConfiguration {
  final def apply[
    K <: Product : Codec,
    V <: Product : Codec,
    S <: Product : Codec : HashableState
  ](
     queryBuilder: S => Query0[V]
   )(defaultState: S, keyExtract: V => K, stateFoldM: S => QueryResult[V] => UIO[S]): DoobieConfiguration[K, V, S] = {
    val qBuilder = new QueryBuilder[V, S] {
      override val queryId: Int = queryBuilder(defaultState).sql.hashCode

      override def query(state: S): Query0[V] = queryBuilder(state)
    }
    new DoobieConfiguration[K, V, S](queryBuilder = qBuilder, defaultState = defaultState, keyExtract = keyExtract, stateFoldM = stateFoldM)
  }

  final def fromTimeSegment[K <: Product : Codec, V <: Product with Timestamped : Ordering : Codec](
                                                                                                     queryBuilder: TimeSegment => Query0[V]
                                                                                                   )(earliest: Instant, tumblingStep: Duration, keyExtract: V => K): DoobieConfiguration[K, V, TimeSegment] = {

    val timeSegment = TimeSegment(earliest, earliest.plus(tumblingStep))

    def stateFold(timeSegment: TimeSegment)(queryResult: QueryResult[V]): UIO[TimeSegment] =
      if (queryResult.results.isEmpty) timeSegment.to.plus(tumblingStep).orNow.map(TimeSegment(timeSegment.from, _))
      else {
        val mostRecent = queryResult.results.max.timestamp
        mostRecent.plus(tumblingStep).orNow.map(TimeSegment(mostRecent, _))
      }

    DoobieConfiguration(queryBuilder)(timeSegment, keyExtract, stateFold)
  }
}
