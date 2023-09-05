package tamer
package db

import cats.syntax.all._
import ciris.{ConfigException, env}
import zio._

import zio.interop.catz._

final case class DbConfig(driver: String, uri: String, username: String, password: String, fetchChunkSize: Int)
object DbConfig {
  private[this] final val dbConfigValue =
    (env("DATABASE_DRIVER"), env("DATABASE_URL"), env("DATABASE_USERNAME"), env("DATABASE_PASSWORD").redacted, env("QUERY_FETCH_CHUNK_SIZE").as[Int])
      .mapN(DbConfig.apply)

  final val fromEnvironment: Layer[TamerError, DbConfig] = ZLayer {
    dbConfigValue
      .load[Task]
      .refineToOrDie[ConfigException]
      .mapError(ce => TamerError(ce.error.redacted.show, ce))
  }
}
