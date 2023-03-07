package tamer
package db

import cats.syntax.all._
import ciris.{ConfigException, env}
import zio._

import zio.Clock
import zio.interop.catz._

final case class ConnectionConfig(driver: String, uri: String, username: String, password: String)
final case class QueryConfig(fetchChunkSize: Int)

object DbConfig {
  private[this] final val _configValue = {
    val dbConfigValue =
      (env("DATABASE_DRIVER"), env("DATABASE_URL"), env("DATABASE_USERNAME"), env("DATABASE_PASSWORD").redacted).mapN(ConnectionConfig)
    val queryConfigValue = env("QUERY_FETCH_CHUNK_SIZE").as[Int].map(QueryConfig)

    (dbConfigValue, queryConfigValue).tupled
  }

  final val fromEnvironment: ZLayer[Blocking with Clock, TamerError, ConnectionConfig with QueryConfig] =
    ZIO
      .runtime[Clock with Blocking]
      .flatMap(implicit runtime => _configValue.load[Task])
      .refineToOrDie[ConfigException]
      .mapError(ce => TamerError(ce.error.redacted.show, ce))
      .map { case (dbConfig, queryConfig) => Has(dbConfig) ++ Has(queryConfig) }
      .toLayerMany
}
