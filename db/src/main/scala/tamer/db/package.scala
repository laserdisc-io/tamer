package tamer

import java.sql.SQLException
import java.time.Instant

import cats.effect.Blocker
import doobie.hikari.HikariTransactor
import doobie.util.transactor.Transactor
import zio._
import zio.blocking.Blocking
import zio.duration._
import zio.interop.catz._

import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits._

package object db {
  implicit final class InstantOps(private val instant: Instant) extends AnyVal {
    def orNow(lag: Duration = 0.seconds): Instant = Instant.now() match {
      case now if instant > now => now - lag
      case _                    => instant
    }
    def +(d: Duration): Instant = instant.plus(d)
    def -(d: Duration): Instant = instant.minus(d)
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

  final val dbLayerFromEnvironment: ZLayer[Blocking, TamerError, Has[ConnectionConfig] with Has[QueryConfig] with Has[Transactor[Task]]] = {
    val configs = DbConfig.fromEnvironment
    configs ++ ((ZLayer.requires[Blocking] ++ configs) >>> hikariLayer)
  }
}
