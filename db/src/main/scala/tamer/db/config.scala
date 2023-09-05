package tamer
package db

import zio.Layer
import zio.config.{ConfigDescriptor, ZConfig}

final case class DbConfig(driver: String, uri: String, username: String, password: String, fetchChunkSize: Int)
object DbConfig {
  val configDescriptor: ConfigDescriptor[DbConfig] =
    (
      ConfigDescriptor.string("DATABASE_DRIVER") zip
        ConfigDescriptor.string("DATABASE_URL") zip
        ConfigDescriptor.string("DATABASE_USERNAME") zip
        ConfigDescriptor.string("DATABASE_PASSWORD") zip
        ConfigDescriptor.int("QUERY_FETCH_CHUNK_SIZE")
    ).to[DbConfig]

  final val fromEnvironment: Layer[TamerError, DbConfig] = ZConfig.fromSystemEnv(configDescriptor).mapError(e => TamerError(e.prettyPrint(), e))
}
