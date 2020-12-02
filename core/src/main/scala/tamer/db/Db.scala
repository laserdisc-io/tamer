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
import tamer.config.{DbConfig, QueryConfig}
import zio._
import zio.interop.catz._
import tamer.db.Compat.toIterable

import scala.concurrent.ExecutionContext

trait Db extends Serializable {
  val db: Db.Service[Any]
}

object Db {
  implicit class InstantOps(ours: Instant) {
    def -(theirs: Instant): Long = ours.toEpochMilli - theirs.toEpochMilli
  }

  case class ChunkWithMetadata[V](chunk: Chunk[V], pulledAt: Instant = Instant.now())
  case class ValueWithMetadata[V](value: V, pulledAt: Instant = Instant.now())

  trait Service[R] {
    def runQuery[K, V, State](
        tnx: Transactor[Task],
        setup: Setup[K, V, State],
        queryConfig: QueryConfig
    )(state: State, q: Queue[(K, V)]): ZIO[R, DbError, State]
  }

  object > extends Service[Db] {
    override final def runQuery[K, V, State](
        tnx: Transactor[Task],
        setup: Setup[K, V, State],
        queryConfig: QueryConfig
    )(state: State, q: Queue[(K, V)]): ZIO[Db, DbError, State] = ZIO.accessM(_.db.runQuery(tnx, setup, queryConfig)(state, q))
  }

  trait Live extends Db {
    override final val db: Service[Any] = new Service[Any] {
      private[this] val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.Db.Live")
      override final def runQuery[K, V, State](
          tnx: Transactor[Task],
          setup: Setup[K, V, State],
          queryConfig: QueryConfig
      )(state: State, q: Queue[(K, V)]): IO[DbError, State] =
        (for {
          log   <- logTask
          query <- UIO(setup.buildQuery(state))
          _     <- log.debug(s"running ${query.sql} with params derived from $state").ignore
          start <- UIO(Instant.now())
          values <-
            query
              .streamWithChunkSize(queryConfig.fetchChunkSize)
              .chunks
              .transact(tnx)
              .map(c => ChunkWithMetadata(c))
              .evalTap(c => q.offerAll(toIterable(c.chunk).map(v => setup.valueToKey(v) -> v)))
              .flatMap(c => Stream.chunk(c.chunk).map(v => ValueWithMetadata(v, c.pulledAt)))
              .compile
              .toList
          newState <- setup.stateFoldM(state)(
            QueryResult(
              ResultMetadata(values.headOption.fold(Instant.now())(_.pulledAt) - start),
              values.map(_.value)
            )
          )
        } yield newState).mapError(e => DbError(e.getLocalizedMessage))
    }
  }

  def mkTransactor(db: DbConfig, connectEC: ExecutionContext, transactEC: ExecutionContext): Managed[DbError, HikariTransactor[Task]] =
    Managed {
      HikariTransactor
        .newHikariTransactor[Task](db.driver, db.uri, db.username, db.password, connectEC, Blocker.liftExecutionContext(transactEC))
        .allocated
        .map { case (ht, cleanup) =>
          Reservation(ZIO.succeed(ht), _ => cleanup.orDie)
        }
        .uninterruptible
        .refineToOrDie[SQLException]
        .mapError(sqle => DbError(sqle.getLocalizedMessage()))
    }
}
