package tamer.db

import ciris.{ConfigException, env}
import ciris.refined.refTypeConfigDecoder
import cats.implicits._
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import tamer.TamerError
import tamer.config.{Password, UriString}
import zio.interop.catz.{taskConcurrentInstance, zioContextShift}
import zio.{Has, Layer, Task, URIO, ZIO, ZLayer}

object ConfigDb {
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

  private[this] val deleteMeConfigValue = (dbConfigValue, queryConfigValue).parMapN(DatabaseConfig.apply)

  trait Service {
    val dbConfig: URIO[DbConfig, Db]
    val queryConfig: URIO[QueryConfig, Query]
  }

  val live: Layer[TamerError, Has[Db] with Has[Query]] = ZLayer.fromEffectMany {
    deleteMeConfigValue.load[Task].refineToOrDie[ConfigException].mapError(ce => TamerError(ce.error.redacted.show, ce)).map {
      case DatabaseConfig(db, query) => Has(db) ++ Has(query)
    }
  }
}
