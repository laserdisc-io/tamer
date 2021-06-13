package tamer
package db

import cats.syntax.all._
import ciris.{ConfigException, env}
import zio.interop.catz._
import zio.{Has, Layer, Task}

final case class ConnectionConfig(driver: String, uri: String, username: String, password: String)
final case class QueryConfig(fetchChunkSize: Int)
final case class DbConfig(connection: ConnectionConfig, query: QueryConfig)

object DbConfig {
  private[this] final val _configValue = {
    val dbConfigValue = (
      env("DATABASE_DRIVER").as[String],
      env("DATABASE_URL").as[String],
      env("DATABASE_USERNAME").as[String],
      env("DATABASE_PASSWORD").as[String].redacted
    ).mapN(ConnectionConfig)
    val queryConfigValue = env("QUERY_FETCH_CHUNK_SIZE").as[Int].map(QueryConfig)

    (dbConfigValue, queryConfigValue).mapN(DbConfig.apply)
  }

  final lazy val fromEnvironment: Layer[TamerError, Has[ConnectionConfig] with Has[QueryConfig]] =
    _configValue
      .load[Task]
      .refineToOrDie[ConfigException]
      .mapError(ce => TamerError(ce.error.redacted.show, ce))
      .map { case DbConfig(connection, query) =>
        Has(connection) ++ Has(query)
      }
      .toLayerMany
}
