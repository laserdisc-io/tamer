package tamer

import cats.effect.Blocker
import com.sksamuel.avro4s.Codec
import doobie.Query0
import doobie.hikari.HikariTransactor
import doobie.implicits.{toDoobieStreamOps, _}
import doobie.util.transactor.Transactor
import eu.timepit.refined.auto._
import fs2.Stream
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.config.Config
import tamer.db.Compat.toIterable
import tamer.db.ConfigDb.{DbConfig, QueryConfig}
import tamer.kafka.Kafka
import zio.blocking.Blocking
import zio.interop.catz._
import zio.{Queue, Task, UIO, ZIO, _}

import java.sql.SQLException
import java.time.{Duration, Instant}
import scala.concurrent.ExecutionContext

package object db {
  final type DbTransactor = Has[Transactor[Task]]
  final type TamerDBConfig = DbTransactor with QueryConfig

  private final lazy val kafkaLayer: Layer[TamerError, Kafka] = Config.live >>> Kafka.live
  private final lazy val transactorLayer: Layer[TamerError, DbTransactor] = (Blocking.live ++ ConfigDb.live) >>> db.hikariLayer
  private final lazy val queryConfigLayer: Layer[TamerError, DbConfig with QueryConfig] = ConfigDb.live
  private final lazy val defaultLayer: Layer[TamerError, DbTransactor with Kafka with QueryConfig] = transactorLayer ++ queryConfigLayer ++ kafkaLayer

  implicit final class InstantOps(private val instant: Instant) extends AnyVal {
    def orNow: UIO[Instant] =
      UIO(Instant.now).map {
        case now if instant.isAfter(now) => now
        case _ => instant
      }
  }

  private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.db")

  private final def iteration[K <: Product, V <: Product, S <: Product : HashableState](
                                                                                         setup: DoobieConfiguration[K, V, S]
                                                                                       )(state: S, q: Queue[(K, V)]): ZIO[TamerDBConfig, TamerError, S] =
    (for {
      log <- logTask
      cfg <- ConfigDb.queryConfig
      tnx <- ZIO.service[Transactor[Task]]
      query <- UIO(setup.queryBuilder.query(state))
      _ <- log.debug(s"running ${query.sql} with params derived from $state")
      start <- UIO(Instant.now())
      values <-
        query
          .streamWithChunkSize(cfg.fetchChunkSize)
          .chunks
          .transact(tnx)
          .map(ChunkWithMetadata(_))
          .evalTap(c => q.offerAll(toIterable(c.chunk).map(v => setup.keyExtract(v) -> v)))
          .flatMap(c => Stream.chunk(c.chunk).map(ValueWithMetadata(_, c.pulledAt)))
          .compile
          .toList
      pulledTimeOrNow = values.headOption.map(_.pulledAt).getOrElse(Instant.now())
      newState <- setup.stateFoldM(state)(
        QueryResult(
          ResultMetadata(
            Duration.between(start, pulledTimeOrNow).toMillis
          ),
          values.map(_.value)
        )
      )
    } yield newState).mapError(e => TamerError(e.getLocalizedMessage, e))

  final def fetchWithTimeSegment[K <: Product : Codec, V <: Product with Timestamped : Ordering : Codec](
                                                                                                          queryBuilder: TimeSegment => Query0[V]
                                                                                                        )(earliest: Instant, tumblingStep: Duration, keyExtract: V => K): ZIO[ZEnv, TamerError, Unit] = {
    val setup = DoobieConfiguration.fromTimeSegment[K, V](queryBuilder)(earliest, tumblingStep, keyExtract)
    fetch(setup)
  }

  final def fetch[
    K <: Product : Codec,
    V <: Product : Codec,
    S <: Product : Codec : HashableState
  ](
     setup: DoobieConfiguration[K, V, S]
   ): ZIO[ZEnv, TamerError, Unit] =
    tamer.kafka.runLoop(setup.generic)(iteration(setup)).provideCustomLayer(defaultLayer)

  final val hikariLayer: ZLayer[Blocking with DbConfig, TamerError, DbTransactor] = ZLayer.fromManaged {
    for {
      cfg <- ConfigDb.dbConfig.toManaged_
      connectEC <- ZIO.descriptor.map(_.executor.asEC).toManaged_
      blockingEC <- blocking.blocking(ZIO.descriptor.map(_.executor.asEC)).toManaged_
      managedTransactor <- mkTransactor(cfg, connectEC, blockingEC)
    } yield managedTransactor
  }

  private final def mkTransactor(
                                  db: ConfigDb.Db,
                                  connectEC: ExecutionContext,
                                  transactEC: ExecutionContext
                                ): Managed[TamerError, HikariTransactor[Task]] =
    HikariTransactor
      .newHikariTransactor[Task](db.driver, db.uri, db.username, db.password, connectEC, Blocker.liftExecutionContext(transactEC))
      .toManagedZIO
      .refineToOrDie[SQLException]
      .mapError(sqle => TamerError(sqle.getLocalizedMessage, sqle))
}
