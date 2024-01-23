package tamer

import java.sql.SQLException
import java.time.Instant

import doobie.hikari.HikariTransactor
import doobie.hikari.HikariTransactor.newHikariTransactor
import doobie.util.transactor.Transactor
import zio._
import zio.interop.catz._

import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits._

package object db {
  implicit final class InstantOps(private val instant: Instant) extends AnyVal {
    def or(now: Instant, lag: Duration = 0.seconds): Instant = if (instant > now) now - lag else instant
    def +(d: Duration): Instant                              = instant.plus(d)
    def -(d: Duration): Instant                              = instant.minus(d)
  }

  final val hikariLayer: RLayer[DbConfig, Transactor[Task]] =
    ZLayer.scoped {
      ZIO
        .service[DbConfig]
        .zip(ZIO.descriptor.map(_.executor.asExecutionContext))
        .flatMap { case (config, ec) => mkTransactor(config, ec) }
    }

  final def mkTransactor(config: DbConfig, connectEC: ExecutionContext): RIO[Scope, HikariTransactor[Task]] =
    newHikariTransactor[Task](config.driver, config.uri, config.username, config.password.value.asString, connectEC).toScopedZIO
      .refineToOrDie[SQLException]
      .mapError(sqle => TamerError(sqle.getLocalizedMessage, sqle))

  final val dbLayerFromEnvironment: TaskLayer[DbConfig with Transactor[Task]] =
    DbConfig.fromEnvironment >+> hikariLayer
}
