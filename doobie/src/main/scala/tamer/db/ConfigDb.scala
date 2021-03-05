package tamer
package db

import cats.implicits._
import ciris.refined.refTypeConfigDecoder
import ciris.{ConfigException, env}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.Uri
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import zio.interop.catz.{taskConcurrentInstance, zioContextShift}
import zio.{Has, Layer, Task, URIO, ZIO, ZLayer}

object ConfigDb {
  type Password    = String
  type UriString   = String Refined Uri
  type DbConfig    = Has[Db]
  type QueryConfig = Has[Query]

  val dbConfig: URIO[DbConfig, Db]          = ZIO.service
  val queryConfig: URIO[QueryConfig, Query] = ZIO.service

  final case class Db(driver: NonEmptyString, uri: UriString, username: NonEmptyString, password: Password)
  final case class Query(fetchChunkSize: PosInt)
  final case class DatabaseConfig(db: Db, query: Query)

  private[this] val dbConfigValue = (
    env("DATABASE_DRIVER").as[NonEmptyString],
    env("DATABASE_URL").as[UriString],
    env("DATABASE_USERNAME").as[NonEmptyString],
    env("DATABASE_PASSWORD").as[Password].redacted
  ).parMapN(Db)
  private[this] val queryConfigValue = env("QUERY_FETCH_CHUNK_SIZE").as[PosInt].map(Query)

  private[this] val configValue = (dbConfigValue, queryConfigValue).parMapN(DatabaseConfig.apply)

  lazy val live: Layer[TamerError, DbConfig with QueryConfig] = ZLayer.fromEffectMany {
    configValue.load[Task].refineToOrDie[ConfigException].mapError(ce => TamerError(ce.error.redacted.show, ce)).map {
      case DatabaseConfig(db, query) => Has(db) ++ Has(query)
    }
  }
}


