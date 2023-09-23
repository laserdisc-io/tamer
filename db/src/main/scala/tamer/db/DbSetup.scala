package tamer
package db

import java.time.Instant

import doobie.Query0
import doobie.implicits._
import doobie.util.transactor.Transactor
import fs2.Stream
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._
import zio.interop.catz._

sealed abstract case class DbSetup[K: Tag, V: Tag, SV: Tag: Hashable](
    initialState: SV,
    recordKey: (SV, V) => K,
    query: SV => Query0[V],
    stateFold: (SV, QueryResult[V]) => UIO[SV]
)(
    implicit ev: SerdesProvider[K, V, SV]
) extends Setup[Transactor[Task] with DbConfig, K, V, SV] {

  private[this] final val sql              = query(initialState).sql
  private[this] final val queryHash        = sql.hash
  private[this] final val initialStateHash = initialState.hash

  override final val stateKey = queryHash + initialStateHash
  override final val repr: String =
    s"""query:              $sql
       |query hash:         $queryHash
       |initial state:      $initialState
       |initial state hash: $initialStateHash
       |state key:          $stateKey
       |""".stripMargin

  import compat._

  private[this] final val logTask = log4sFromName.provideEnvironment(ZEnvironment("tamer.db"))

  private[this] final def process(query: Query0[V], chunkSize: Int, tx: Transactor[Task], queue: Enqueue[NonEmptyChunk[(K, V)]], state: SV) =
    query
      .streamWithChunkSize(chunkSize)
      .chunks
      .transact(tx)
      .map(ChunkWithMetadata(_))
      .evalTap { c =>
        val chunk = Chunk.fromIterable(c.chunk.toStream.map(v => recordKey(state, v) -> v))
        NonEmptyChunk.fromChunk(chunk).map(queue.offer).getOrElse(ZIO.unit)
      }
      .flatMap(c => Stream.chunk(c.chunk).map(ValueWithMetadata(_, c.pulledAt)))
      .compile
      .toList
      .map(values => values -> values.headOption.map(_.pulledAt).getOrElse(java.lang.System.nanoTime()))

  override def iteration(currentState: SV, queue: Enqueue[NonEmptyChunk[(K, V)]]): RIO[Transactor[Task] with DbConfig, SV] = for {
    log        <- logTask
    transactor <- ZIO.service[Transactor[Task]]
    chunkSize  <- ZIO.service[DbConfig].map(_.fetchChunkSize)
    query      <- ZIO.succeed(query(currentState))
    _          <- log.debug(s"running ${query.sql} with params derived from $currentState")
    start      <- Clock.nanoTime
    tuple      <- process(query, chunkSize, transactor, queue, currentState)
    (values, time) = tuple
    newState <- stateFold(currentState, QueryResult(ResultMetadata(time - start), values.map(_.value)))
  } yield newState
}

object DbSetup {
  final def apply[K: Tag, V: Tag, SV: Tag: Hashable](
      initialState: SV
  )(
      query: SV => Query0[V]
  )(
      recordKey: (SV, V) => K,
      stateFold: (SV, QueryResult[V]) => UIO[SV]
  )(
      implicit ev: SerdesProvider[K, V, SV]
  ): DbSetup[K, V, SV] = new DbSetup(initialState, recordKey, query, stateFold) {}

  final def tumbling[K: Tag, V <: Timestamped: Tag: Ordering](
      query: Window => Query0[V]
  )(
      recordKey: (Window, V) => K,
      from: Instant = Instant.now,
      tumblingStep: Duration = 5.minutes,
      lag: Duration = 0.seconds
  )(
      implicit ev: SerdesProvider[K, V, Window]
  ): DbSetup[K, V, Window] = {
    def stateFold(currentWindow: Window, queryResult: QueryResult[V]): UIO[Window] =
      if (queryResult.results.isEmpty) {
        Clock.instant.map(now => Window(currentWindow.from, (currentWindow.to + tumblingStep).or(now, lag)))
      } else {
        val mostRecent = queryResult.results.max.timestamp
        Clock.instant.map(now => Window(mostRecent, (mostRecent + tumblingStep).or(now, lag)))
      }

    DbSetup(Window(from, from + tumblingStep))(query)(recordKey, stateFold)
  }
}
