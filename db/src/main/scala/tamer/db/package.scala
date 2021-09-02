package tamer

import java.sql.SQLException
import java.time.Instant

import doobie.hikari.HikariTransactor.newHikariTransactor
import doobie.util.transactor.Transactor
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
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

  final val hikariLayer: ZLayer[Blocking with Clock with Has[ConnectionConfig], TamerError, Has[Transactor[Task]]] =
    (ZManaged
      .service[ConnectionConfig]
      .zip(ZIO.descriptor.map(_.executor.asEC).toManaged_))
      .flatMap { case (config, ec) => mkTransactor(config, ec) }
      .toLayer

  private final def mkTransactor(config: ConnectionConfig, connectEC: ExecutionContext): ZManaged[Blocking with Clock, TamerError, Transactor[Task]] =
    ZManaged
      .runtime[Blocking with Clock]
      .flatMap(implicit runtime => newHikariTransactor[Task](config.driver, config.uri, config.username, config.password, connectEC).toManagedZIO)
      .refineToOrDie[SQLException]
      .mapError(sqle => TamerError(sqle.getLocalizedMessage, sqle))

  final val dbLayerFromEnvironment: ZLayer[Blocking with Clock, TamerError, Has[ConnectionConfig] with Has[QueryConfig] with Has[Transactor[Task]]] =
    (ZLayer.requires[Blocking with Clock] ++ DbConfig.fromEnvironment) >+> hikariLayer
}
