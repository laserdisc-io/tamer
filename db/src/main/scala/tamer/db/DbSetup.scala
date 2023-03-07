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

sealed abstract case class DbSetup[K, V, S: Hashable](
    serdes: Setup.Serdes[K, V, S],
    initialState: S,
    recordKey: (S, V) => K,
    query: S => Query0[V],
    stateFold: (S, QueryResult[V]) => UIO[S]
) extends Setup[Transactor[Task] with QueryConfig, K, V, S] {

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

  private[this] final val logTask = log4sFromName.provideService("tamer.db")

  private[this] final def process(query: Query0[V], chunkSize: Int, tx: Transactor[Task], queue: Enqueue[NonEmptyChunk[(K, V)]], state: S) =
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
      .map(values => values -> values.headOption.map(_.pulledAt).getOrElse(System.nanoTime()))

  override def iteration(currentState: S, queue: Enqueue[NonEmptyChunk[(K, V)]]): RIO[Transactor[Task] with QueryConfig, S] = for {
    log            <- logTask
    transactor     <- ZIO.service[Transactor[Task]]
    chunkSize      <- ZIO.service[QueryConfig].map(_.fetchChunkSize)
    query          <- ZIO.succeed(query(currentState))
    _              <- log.debug(s"running ${query.sql} with params derived from $currentState")
    start          <- ZIO.succeed(System.nanoTime())
    (values, time) <- process(query, chunkSize, transactor, queue, currentState)
    newState       <- stateFold(currentState, QueryResult(ResultMetadata(time - start), values.map(_.value)))
  } yield newState
}

object DbSetup {
  final def apply[K: Codec, V: Codec, S: Codec: Hashable](
      initialState: S
  )(
      query: S => Query0[V]
  )(
      recordKey: (S, V) => K,
      stateFold: (S, QueryResult[V]) => UIO[S]
  )(
      implicit ev: Codec[Tamer.StateKey]
  ): DbSetup[K, V, S] = new DbSetup(Setup.mkSerdes[K, V, S], initialState, recordKey, query, stateFold) {}

  final def tumbling[K: Codec, V <: Timestamped: Ordering: Codec](
      query: Window => Query0[V]
  )(
      recordKey: (Window, V) => K,
      from: Instant = Instant.now,
      tumblingStep: Duration = 5.minutes,
      lag: Duration = 0.seconds
  )(
      implicit ev0: Codec[Tamer.StateKey],
      ev1: Codec[Window]
  ): DbSetup[K, V, Window] = {
    def stateFold(currentWindow: Window, queryResult: QueryResult[V]): UIO[Window] =
      if (queryResult.results.isEmpty) {
        ZIO.succeed((currentWindow.to + tumblingStep).orNow(lag)).map(Window(currentWindow.from, _))
      } else {
        val mostRecent = queryResult.results.max.timestamp
        ZIO.succeed((mostRecent + tumblingStep).orNow(lag)).map(Window(mostRecent, _))
      }

    DbSetup(Window(from, from + tumblingStep))(query)(recordKey, stateFold)
  }
}
