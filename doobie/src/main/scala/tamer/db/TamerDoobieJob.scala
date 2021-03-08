package tamer.db

import com.sksamuel.avro4s.Codec
import doobie.Query0
import doobie.implicits.{toDoobieStreamOps, _}
import doobie.util.transactor.Transactor
import fs2.Stream
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.config.{Config, KafkaConfig}
import tamer.db.Compat.toIterable
import tamer.db.ConfigDb.{DbConfig, QueryConfig}
import tamer.job.AbstractTamerJob
import tamer.{TamerError, db}
import zio.blocking.Blocking
import zio.interop.catz.taskConcurrentInstance
import zio.{Chunk, Layer, Queue, Task, UIO, ZEnv, ZIO}

import java.time.{Duration, Instant}

object TamerDoobieJob {
  def apply[
      R <: ZEnv with DbConfig with TamerDBConfig with KafkaConfig,
      K <: Product: Codec,
      V <: Product: Codec,
      S <: Product: Codec
  ](
      setup: DoobieConfiguration[K, V, S]
  ) = new TamerDoobieJob[R, K, V, S](setup)

  final def fetchWithTimeSegment[K <: Product: Codec, V <: Product with Timestamped: Ordering: Codec](
      queryBuilder: TimeSegment => Query0[V]
  )(earliest: Instant, tumblingStep: Duration, keyExtract: V => K): ZIO[ZEnv, TamerError, Unit] = {
    val transactorLayer: Layer[TamerError, DbTransactor]               = (Blocking.live ++ ConfigDb.live) >>> db.hikariLayer
    val queryConfigLayer: Layer[TamerError, DbConfig with QueryConfig] = ConfigDb.live
    val setup: DoobieConfiguration[K, V, TimeSegment]                  = DoobieConfiguration.fromTimeSegment[K, V](queryBuilder)(earliest, tumblingStep, keyExtract)
    apply(setup).fetch().provideSomeLayer[ZEnv](transactorLayer ++ queryConfigLayer ++ Config.live)
  }

}
class TamerDoobieJob[
    R <: ZEnv with DbConfig with TamerDBConfig with KafkaConfig,
    K <: Product: Codec,
    V <: Product: Codec,
    S <: Product: Codec
](
    setup: DoobieConfiguration[K, V, S]
) extends AbstractTamerJob[R, K, V, S](setup.generic) {
  import eu.timepit.refined.auto._

  private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.db")


  override protected def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] = {
    (for {
      log   <- logTask
      cfg   <- ConfigDb.queryConfig
      tnx   <- ZIO.service[Transactor[Task]]
      query <- UIO(setup.queryBuilder.query(currentState))
      _     <- log.debug(s"running ${query.sql} with params derived from $currentState")
      start <- UIO(Instant.now())
      values <-
        query
          .streamWithChunkSize(cfg.fetchChunkSize)
          .chunks
          .transact(tnx)
          .map(ChunkWithMetadata(_))
          .evalTap(c => q.offer(Chunk.fromIterable(toIterable(c.chunk).map(v => setup.keyExtract(v) -> v))))
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
}
