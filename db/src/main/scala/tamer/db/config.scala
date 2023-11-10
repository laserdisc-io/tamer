package tamer
package db

import zio._

final case class DbConfig(driver: String, uri: String, username: String, password: Config.Secret, fetchChunkSize: Int)
object DbConfig {
  private[this] final val dbConfigValue =
    (
      Config.string("database_driver") ++
        Config.string("database_url") ++
        Config.string("database_username") ++
        Config.secret("database_password") ++
        Config.int("query_fetch_chunk_size")
    ).map { case (driver, uri, username, password, fetchChunkSize) =>
      DbConfig(driver, uri, username, password, fetchChunkSize)
    }

  final val fromEnvironment: Layer[TamerError, DbConfig] = ZLayer {
    ZIO.config(dbConfigValue).mapError(ce => TamerError(ce.getMessage(), ce))
  }
}
