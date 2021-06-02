package tamer

import cats.effect.Blocker
import doobie.hikari.HikariTransactor
import doobie.util.transactor.Transactor
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.string.Uri
import zio._
import zio.blocking.Blocking
import zio.interop.catz._

import java.sql.SQLException
import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

package object db {
  type Password  = String
  type UriString = String Refined Uri

  implicit final class InstantOps(private val instant: Instant) extends AnyVal {
    def orNow: UIO[Instant] = UIO(Instant.now).map {
      case now if instant.isAfter(now) => now
      case _                           => instant
    }
    def +(d: FiniteDuration): Instant = instant.plusMillis(d.toMillis)
    def -(d: FiniteDuration): Instant = instant.minusMillis(d.toMillis)
  }

  final val hikariLayer: ZLayer[Blocking with Has[ConnectionConfig], TamerError, Has[Transactor[Task]]] = ZLayer.fromManaged {
    for {
      cfg               <- ZIO.service[ConnectionConfig].toManaged_
      connectEC         <- ZIO.descriptor.map(_.executor.asEC).toManaged_
      blockingEC        <- blocking.blocking(ZIO.descriptor.map(_.executor.asEC)).toManaged_
      managedTransactor <- mkTransactor(cfg, connectEC, blockingEC)
    } yield managedTransactor
  }

  private final def mkTransactor(
      config: ConnectionConfig,
      connectEC: ExecutionContext,
      transactEC: ExecutionContext
  ): Managed[TamerError, HikariTransactor[Task]] =
    HikariTransactor
      .newHikariTransactor[Task](config.driver, config.uri, config.username, config.password, connectEC, Blocker.liftExecutionContext(transactEC))
      .toManagedZIO
      .refineToOrDie[SQLException]
      .mapError(sqle => TamerError(sqle.getLocalizedMessage, sqle))
}
