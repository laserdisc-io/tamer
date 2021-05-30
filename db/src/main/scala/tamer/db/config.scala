package tamer
package db

import cats.syntax.all._
import ciris.refined.refTypeConfigDecoder
import ciris.{ConfigException, env}
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import zio.interop.catz.{taskConcurrentInstance, zioContextShift}
import zio.{Has, Layer, Task}

final case class ConnectionConfig(driver: NonEmptyString, uri: UriString, username: NonEmptyString, password: Password)
final case class QueryConfig(fetchChunkSize: PosInt)
final case class DbConfig(connection: ConnectionConfig, query: QueryConfig)

object DbConfig {
  private[this] final val _configValue = {
    val dbConfigValue = (
      env("DATABASE_DRIVER").as[NonEmptyString],
      env("DATABASE_URL").as[UriString],
      env("DATABASE_USERNAME").as[NonEmptyString],
      env("DATABASE_PASSWORD").as[Password].redacted
    ).mapN(ConnectionConfig)
    val queryConfigValue = env("QUERY_FETCH_CHUNK_SIZE").as[PosInt].map(QueryConfig)

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
