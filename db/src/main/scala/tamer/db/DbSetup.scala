package tamer
package db

import com.sksamuel.avro4s.Codec
import doobie.Query0
import zio.UIO

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

sealed abstract trait QueryBuilder[-S, V] {

  /** Used for hashing purposes
    */
  val queryId: Int

  def query(state: S): Query0[V]
}

final case class ResultMetadata(queryExecutionTime: Long)

final case class QueryResult[V](metadata: ResultMetadata, results: List[V])

sealed abstract case class DbSetup[K: Codec, V: Codec, S: Codec: Hashable](
    defaultState: S,
    queryBuilder: QueryBuilder[S, V],
    keyExtract: V => K,
    stateFoldM: S => QueryResult[V] => UIO[S]
) {
  private val keyId = queryBuilder.queryId + Hashable[S].hash(defaultState)
  private val repr: String =
    s"""
       |query:             ${queryBuilder.query(defaultState).sql}
       |query id:          ${queryBuilder.queryId}
       |default state:     $defaultState
       |default state id:  ${Hashable[S].hash(defaultState)}
       |default state key: $keyId
       |""".stripMargin.stripLeading()

  val generic: Setup[K, V, S] = Setup(Setup.Serdes[K, V, S], defaultState, keyId, repr)
}

object DbSetup {
  final def apply[K: Codec, V: Codec, S: Codec: Hashable](
      defaultState: S
  )(
      queryBuilder: S => Query0[V]
  )(
      keyExtract: V => K,
      stateFoldM: S => QueryResult[V] => UIO[S]
  ): DbSetup[K, V, S] = {
    val qb = new QueryBuilder[S, V] {
      override val queryId: Int = queryBuilder(defaultState).sql.hashCode

      override def query(state: S): Query0[V] = queryBuilder(state)
    }
    new DbSetup(defaultState, qb, keyExtract, stateFoldM) {}
  }

  final def fromTimeSegment[K: Codec, V <: Timestamped: Ordering: Codec](
      queryBuilder: TimeSegment => Query0[V]
  )(earliest: Instant, tumblingStep: FiniteDuration, keyExtract: V => K): DbSetup[K, V, TimeSegment] = {

    val defaultTimeSegment = TimeSegment(earliest, earliest + tumblingStep)

    def stateFold(currentTimeSegment: TimeSegment)(queryResult: QueryResult[V]): UIO[TimeSegment] =
      if (queryResult.results.isEmpty) {
        (currentTimeSegment.to + tumblingStep).orNow.map(TimeSegment(currentTimeSegment.from, _))
      } else {
        val mostRecent = queryResult.results.max.timestamp
        (mostRecent + tumblingStep).orNow.map(TimeSegment(mostRecent, _))
      }

    DbSetup(defaultTimeSegment)(queryBuilder)(keyExtract, stateFold)
  }
}
