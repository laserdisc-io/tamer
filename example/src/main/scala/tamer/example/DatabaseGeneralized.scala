package tamer.example

import doobie.implicits.legacy.instant._
import doobie.syntax.string._
import tamer.{HashableState, TamerError, db}
import tamer.config.Config
import tamer.db.ConfigDb.{DbConfig, QueryConfig}
import tamer.db.{ConfigDb, DbTransactor, InstantOps, QueryResult, DoobieConfiguration, TamerDBConfig}
import tamer.kafka.Kafka
import zio._
import zio.blocking.Blocking

import java.time.Instant
import java.time.temporal.ChronoUnit._
import scala.util.hashing.byteswap64

final case class MyState(from: Instant, to: Instant) extends HashableState {

  /** It is required for this hash to be consistent even across executions
    * for the same semantic state. This is in contrast with the built-in
    * `hashCode` method.
    */
  override val stateHash: Int = (byteswap64(from.getEpochSecond) + byteswap64(to.getEpochSecond)).intValue
}

object DatabaseGeneralized extends zio.App {
  lazy val transactorLayer: Layer[TamerError, DbTransactor]                     = (Blocking.live ++ ConfigDb.live) >>> db.hikariLayer
  lazy val kafkaLayer: Layer[TamerError, Kafka]                                 = Config.live >>> Kafka.live
  lazy val queryConfigLayer: Layer[TamerError, DbConfig with QueryConfig]       = ConfigDb.live
  lazy val myLayer: Layer[TamerError, DbTransactor with Kafka with QueryConfig] = transactorLayer ++ kafkaLayer ++ queryConfigLayer
  lazy val program: ZIO[Kafka with TamerDBConfig with ZEnv, TamerError, Unit] = (for {
    boot <- UIO(Instant.now())
    earliest = boot.minus(60, DAYS)
    setup = DoobieConfiguration((s: MyState) =>
      sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${s.from} AND modified_at <= ${s.to}""".query[Value]
    )(
      defaultState = MyState(earliest, earliest.plus(5, MINUTES)),
      keyExtract = (value: Value) => Key(value.id),
      stateFoldM = (s: MyState) => {
        case QueryResult(_, results) if results.isEmpty => s.to.plus(5, MINUTES).orNow.map(MyState(s.from, _))
        case QueryResult(_, results) =>
          val mostRecent = results.sortBy(_.modifiedAt).max.timestamp
          mostRecent.plus(5, MINUTES).orNow.map(MyState(mostRecent, _))
      }
    )
    _ <- tamer.db.fetch(setup)
  } yield ()).mapError(e => TamerError("Could not run tamer example", e))

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.provideCustomLayer(myLayer).exitCode
}
