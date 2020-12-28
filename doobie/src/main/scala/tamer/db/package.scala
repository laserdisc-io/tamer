package tamer

import cats.effect.Blocker
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import doobie.Query0
import doobie.hikari.HikariTransactor
import doobie.implicits.{toDoobieStreamOps, _}
import doobie.util.transactor.Transactor
import eu.timepit.refined.auto._
import fs2.Stream
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.db.Compat.toIterable
import tamer.db.ConfigDb.{DbConfig, QueryConfig}
import tamer.db.Db.{Datable, TimeSegment, _}
import tamer.kafka.Kafka
import zio.blocking.Blocking
import zio.interop.catz._
import zio.{Queue, Task, UIO, ZIO, _}

import java.sql.SQLException
import java.time.{Duration, Instant}
import scala.concurrent.ExecutionContext

package object db {
  type DbTransactor  = Has[Transactor[Task]]
  type TamerDBConfig = DbTransactor with QueryConfig

  private[this] implicit final class InstantOps(private val instant: Instant) extends AnyVal {
    def orNow: UIO[Instant] =
      UIO(Instant.now).map {
        case now if instant.isAfter(now) => now
        case _                           => instant
      }
  }

  final def mkSetup[K <: Product: Encoder: Decoder: SchemaFor, V <: Product with Datable: Ordering: Encoder: Decoder: SchemaFor](
      queryBuilder: TimeSegment => Query0[V]
  )(earliest: Instant, tumblingStep: Duration, keyExtract: V => K): Setup[K, V, TimeSegment] = {

    val timeSegment = TimeSegment(earliest, earliest.plus(tumblingStep))

    val qBuilder = new QueryBuilder[V, TimeSegment] {
      override val queryId: Int                         = queryBuilder(timeSegment).sql.hashCode
      override def query(state: TimeSegment): Query0[V] = queryBuilder(state)
    }

    def stateFold(timeSegment: TimeSegment)(queryResult: QueryResult[V]): UIO[TimeSegment] =
      if (queryResult.results.isEmpty) timeSegment.to.plus(tumblingStep).orNow.map(TimeSegment(timeSegment.from, _))
      else {
        val mostRecent = queryResult.results.max.instant
        mostRecent.plus(tumblingStep).orNow.map(TimeSegment(mostRecent, _))
      }

    Setup[K, V, TimeSegment](qBuilder, timeSegment, keyExtract, stateFold)
  }

  private[this] val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.db")

  final def iteration[K <: Product, V <: Product](
      setup: Setup[K, V, TimeSegment]
  )(state: TimeSegment, q: Queue[(K, V)]): ZIO[TamerDBConfig, TamerError, TimeSegment] =
    (for {
      log   <- logTask
      cfg   <- ConfigDb.queryConfig
      tnx   <- ZIO.service[Transactor[Task]]
      query <- UIO(setup.queryBuilder.query(state))
      _     <- log.info(s"running ${query.sql} with params derived from $state") // TODO: make this debug
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

  final def fetchWithTimeSegment[K <: Product, V <: Product](
      setup: Setup[K, V, TimeSegment]
  ): ZIO[Kafka with TamerDBConfig with ZEnv, TamerError, Unit] =
    tamer.kafka.runLoop(setup)(iteration(setup))

  val hikariLayer: ZLayer[Blocking with DbConfig, TamerError, DbTransactor] = ZLayer.fromManaged {
    for {
      cfg               <- ConfigDb.dbConfig.toManaged_
      connectEC         <- ZIO.descriptor.map(_.executor.asEC).toManaged_
      blockingEC        <- blocking.blocking(ZIO.descriptor.map(_.executor.asEC)).toManaged_
      managedTransactor <- mkTransactor(cfg, connectEC, blockingEC)
    } yield managedTransactor
  }

  def mkTransactor(db: ConfigDb.Db, connectEC: ExecutionContext, transactEC: ExecutionContext): Managed[TamerError, HikariTransactor[Task]] =
    HikariTransactor
      .newHikariTransactor[Task](db.driver, db.uri, db.username, db.password, connectEC, Blocker.liftExecutionContext(transactEC))
      .toManagedZIO
      .refineToOrDie[SQLException]
      .mapError(sqle => TamerError(sqle.getLocalizedMessage, sqle))
}
