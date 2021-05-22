package tamer
package db

import com.sksamuel.avro4s.Codec
import doobie.Query0
import doobie.implicits.toDoobieStreamOps
import doobie.util.transactor.Transactor
import fs2.Stream
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio.blocking.Blocking
import zio.interop.catz.taskConcurrentInstance
import zio.{Chunk, Has, Queue, Task, UIO, ZEnv, ZIO}

import java.time.{Duration, Instant}
import scala.concurrent.duration.FiniteDuration

object DbTamer {
  def apply[R <: ZEnv with Has[Transactor[Task]] with Has[QueryConfig] with Has[KafkaConfig], K, V, S](setup: DbSetup[K, V, S]) =
    new DbTamer[R, K, V, S](setup)

  final def fetchWithTimeSegment[K: Codec, V <: Timestamped: Ordering: Codec](
      queryBuilder: TimeSegment => Query0[V]
  )(earliest: Instant, tumblingStep: FiniteDuration, keyExtract: V => K): ZIO[ZEnv, TamerError, Unit] = {
    val transactorLayer  = (Blocking.live ++ DbConfig.fromEnvironment) >>> db.hikariLayer
    val queryConfigLayer = DbConfig.fromEnvironment
    val setup            = DbSetup.fromTimeSegment(queryBuilder)(earliest, tumblingStep, keyExtract)
    apply(setup).run.provideSomeLayer[ZEnv](transactorLayer ++ queryConfigLayer ++ KafkaConfig.fromEnvironment)
  }

}
class DbTamer[R <: ZEnv with Has[Transactor[Task]] with Has[QueryConfig] with Has[KafkaConfig], K, V, S](setup: DbSetup[K, V, S])
    extends AbstractTamer[R, K, V, S](setup.generic) {

  import compat._
  import eu.timepit.refined.auto._

  private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.db")

  override protected def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] =
    (for {
      log   <- logTask
      tnx   <- ZIO.service[Transactor[Task]]
      cfg   <- ZIO.service[QueryConfig]
      query <- UIO(setup.queryBuilder.query(currentState))
      _     <- log.debug(s"running ${query.sql} with params derived from $currentState")
      start <- UIO(Instant.now())
      values <-
        query
          .streamWithChunkSize(cfg.fetchChunkSize)
          .chunks
          .transact(tnx)
          .map(ChunkWithMetadata(_))
          .evalTap(c => q.offer(Chunk.fromIterable(c.chunk.toStream.map(v => setup.keyExtract(v) -> v))))
          .flatMap(c => Stream.chunk(c.chunk).map(ValueWithMetadata(_, c.pulledAt)))
          .compile
          .toList
      pulledTimeOrNow = values.headOption.map(_.pulledAt).getOrElse(Instant.now())
      newState <- setup.stateFoldM(currentState)(
        QueryResult(
          ResultMetadata(
            Duration.between(start, pulledTimeOrNow).toMillis
          ),
          values.map(_.value)
        )
      )
    } yield newState).mapError(e => TamerError(e.getLocalizedMessage, e))
}
