/*
 * Copyright (c) 2019-2025 LaserDisc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

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
