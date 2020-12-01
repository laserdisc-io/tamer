package tamer
package db

import java.sql.SQLException
import java.time.Instant

import cats.effect.Blocker
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.util.transactor.Transactor
import eu.timepit.refined.auto._
import fs2.{Chunk, Stream}
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.config._
import zio._
import zio.blocking.Blocking
import zio.interop.catz._

import scala.concurrent.ExecutionContext

object Db {
  implicit class InstantOps(ours: Instant) {
    def -(theirs: Instant): Long = ours.toEpochMilli - theirs.toEpochMilli
  }

  case class ChunkWithMetadata[V](chunk: Chunk[V], pulledAt: Instant = Instant.now())
  case class ValueWithMetadata[V](value: V, pulledAt: Instant = Instant.now())

  trait Service {
    def runQuery[K, V, State](setup: Setup[K, V, State])(state: State, q: Queue[(K, V)]): IO[TamerError, State]
  }

  // https://github.com/zio/zio/issues/2949
  val live: URLayer[DbTransactor with QueryConfig, Db] = ZLayer.fromServices[Transactor[Task], Config.Query, Service] { (tnx, cfg) =>
    new Service {
      private[this] val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.db")
      override final def runQuery[K, V, State](setup: Setup[K, V, State])(state: State, q: Queue[(K, V)]): IO[TamerError, State] =
        (for {
          log   <- logTask
          query <- UIO(setup.buildQuery(state))
          _     <- log.debug(s"running ${query.sql} with params derived from $state").ignore
          start <- UIO(Instant.now())
          values <-
            query
              .streamWithChunkSize(cfg.fetchChunkSize)
              .chunks
              .transact(tnx)
              .map(ChunkWithMetadata(_))
              .evalTap(c => q.offerAll(c.chunk.iterator.to(LazyList).map(v => setup.valueToKey(v) -> v)))
              .flatMap(c => Stream.chunk(c.chunk).map(ValueWithMetadata(_, c.pulledAt)))
              .compile
              .toList
          newState <- setup.stateFoldM(state)(
            QueryResult(
              ResultMetadata(values.headOption.fold(Instant.now())(_.pulledAt) - start),
              values.map(_.value)
            )
          )
        } yield newState).mapError { case e: Exception => TamerError(e.getLocalizedMessage, e) }
    }
  }

  val hikariLayer: ZLayer[Blocking with DbConfig, TamerError, DbTransactor] = ZLayer.fromManaged {
    for {
      cfg               <- dbConfig.toManaged_
      connectEC         <- ZIO.descriptor.map(_.executor.asEC).toManaged_
      blockingEC        <- blocking.blocking(ZIO.descriptor.map(_.executor.asEC)).toManaged_
      managedTransactor <- mkTransactor(cfg, connectEC, blockingEC)
    } yield managedTransactor
  }

  def mkTransactor(db: Config.Db, connectEC: ExecutionContext, transactEC: ExecutionContext): Managed[TamerError, HikariTransactor[Task]] =
    HikariTransactor
      .newHikariTransactor[Task](db.driver, db.uri, db.username, db.password, connectEC, Blocker.liftExecutionContext(transactEC))
      .toManagedZIO
      .refineToOrDie[SQLException]
      .mapError(sqle => TamerError(sqle.getLocalizedMessage, sqle))
}
