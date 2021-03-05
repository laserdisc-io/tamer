package tamer

import cats.effect.Blocker
import doobie.hikari.HikariTransactor
import doobie.util.transactor.Transactor
import eu.timepit.refined.auto._
import tamer.db.ConfigDb.{DbConfig, QueryConfig}
import zio.blocking.Blocking
import zio.interop.catz._
import zio.{Task, ZIO, _}

import java.sql.SQLException
import java.time.Instant
import scala.concurrent.ExecutionContext

package object db {
  final type DbTransactor  = Has[Transactor[Task]]
  final type TamerDBConfig = DbTransactor with QueryConfig

  implicit final class InstantOps(private val instant: Instant) extends AnyVal {
    def orNow: UIO[Instant] =
      UIO(Instant.now).map {
        case now if instant.isAfter(now) => now
        case _                           => instant
      }
  }



  final val hikariLayer: ZLayer[Blocking with DbConfig, TamerError, DbTransactor] = ZLayer.fromManaged {
    for {
      cfg               <- ConfigDb.dbConfig.toManaged_
      connectEC         <- ZIO.descriptor.map(_.executor.asEC).toManaged_
      blockingEC        <- blocking.blocking(ZIO.descriptor.map(_.executor.asEC)).toManaged_
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
