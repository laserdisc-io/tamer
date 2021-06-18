package tamer
package db

import java.time.Instant

import doobie.Query0
import doobie.implicits._
import doobie.util.transactor.Transactor
import fs2.Stream
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._
import zio.duration._
import zio.interop.catz._

sealed abstract case class DbSetup[K, V, S: Hashable](
    serdes: Setup.Serdes[K, V, S],
    defaultState: S,
    query: S => Query0[V],
    recordKey: (S, V) => K,
    stateFold: (S, QueryResult[V]) => UIO[S]
) extends Setup[Has[Transactor[Task]] with Has[QueryConfig], K, V, S] {

  private[this] final val sql     = query(defaultState).sql
  private[this] final val queryId = sql.hash
  private[this] final val stateId = defaultState.hash

  override final val stateKey = queryId + stateId
  override final val repr: String =
    s"""query:             $sql
       |query id:          $queryId
       |default state:     $defaultState
       |default state id:  $stateId
       |default state key: $stateKey
       |""".stripMargin

  import compat._

  private[this] final val logTask = log4sFromName.provide("tamer.db")

  private[this] final def process(query: Query0[V], chunkSize: Int, tx: Transactor[Task], queue: Queue[Chunk[(K, V)]], state: S) =
    query
      .streamWithChunkSize(chunkSize)
      .chunks
      .transact(tx)
      .map(ChunkWithMetadata(_))
      .evalTap(c => queue.offer(Chunk.fromIterable(c.chunk.toStream.map(v => recordKey(state, v) -> v))))
      .flatMap(c => Stream.chunk(c.chunk).map(ValueWithMetadata(_, c.pulledAt)))
      .compile
      .toList
      .map(values => values -> values.headOption.map(_.pulledAt).getOrElse(System.nanoTime()))

  override def iteration(currentState: S, queue: Queue[Chunk[(K, V)]]): RIO[Has[Transactor[Task]] with Has[QueryConfig], S] = for {
    log            <- logTask
    transactor     <- ZIO.service[Transactor[Task]]
    chunkSize      <- ZIO.service[QueryConfig].map(_.fetchChunkSize)
    query          <- UIO(query(currentState))
    _              <- log.debug(s"running ${query.sql} with params derived from $currentState")
    start          <- UIO(System.nanoTime())
    (values, time) <- process(query, chunkSize, transactor, queue, currentState)
    newState       <- stateFold(currentState, QueryResult(ResultMetadata(time - start), values.map(_.value)))
  } yield newState
}

object DbSetup {
  final def apply[K: Codec, V: Codec, S: Codec: Hashable](
      defaultState: S
  )(
      query: S => Query0[V]
  )(
      recordKey: (S, V) => K,
      stateFold: (S, QueryResult[V]) => UIO[S]
  ): DbSetup[K, V, S] = new DbSetup(Setup.Serdes[K, V, S], defaultState, query, recordKey, stateFold) {}

  final def tumbling[K: Codec, V <: Timestamped: Ordering: Codec](
      query: Window => Query0[V]
  )(
      recordKey: (Window, V) => K,
      from: Instant = Instant.now,
      tumblingStep: Duration = 5.minutes,
      lag: Duration = 0.seconds
  )(
      implicit ev: Codec[Window]
  ): DbSetup[K, V, Window] = {
    def stateFold(currentWindow: Window, queryResult: QueryResult[V]): UIO[Window] =
      if (queryResult.results.isEmpty) {
        UIO((currentWindow.to + tumblingStep).orNow(lag)).map(Window(currentWindow.from, _))
      } else {
        val mostRecent = queryResult.results.max.timestamp
        UIO((mostRecent + tumblingStep).orNow(lag)).map(Window(mostRecent, _))
      }

    DbSetup(Window(from, from + tumblingStep))(query)(recordKey, stateFold)
  }
}
