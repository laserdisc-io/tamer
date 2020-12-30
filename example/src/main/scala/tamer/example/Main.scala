package tamer
package example

import tamer.config.Config
import tamer.db.Db.Datable
import tamer.db.{ConfigDb, DbTransactor, HashableState, InstantOps, QueryResult, TamerDBConfig}
import tamer.kafka.Kafka
import zio.blocking.Blocking
import zio._
import doobie.implicits.legacy.instant._
import doobie.syntax.string._
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.db.ConfigDb.{DbConfig, QueryConfig}

import java.time.temporal.ChronoUnit._
import java.time.{Duration, Instant}
import scala.util.hashing.byteswap64

final case class MyState(from: Instant, to: Instant) extends HashableState {

  /** It is required for this hash to be consistent even across executions
    * for the same semantic state. This is in contrast with the built-in
    * `hashCode` method.
    */
  override val stateHash: Int = (byteswap64(from.getEpochSecond) + byteswap64(to.getEpochSecond)).intValue
}
final case class Key(id: String)
final case class Value(id: String, name: String, description: Option[String], modifiedAt: Instant) extends Datable(modifiedAt)
object Value {
  implicit val ordering: Ordering[Value] = (x: Value, y: Value) => x.modifiedAt.compareTo(y.modifiedAt)
}

object Main extends zio.App {
  val program: ZIO[ZEnv, TamerError, Unit] = (for {
    log  <- log4sFromName.provide("tamer.example")
    _    <- log.info("Starting tamer...")
    boot <- UIO(Instant.now())
    _ <- tamer.db.fetchWithTimeSegment(ts =>
      sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${ts.from} AND modified_at <= ${ts.to}""".query[Value]
    )(
      earliest = boot.minus(60, DAYS),
      tumblingStep = Duration.of(5, MINUTES),
      keyExtract = (value: Value) => Key(value.id)
    )
  } yield ()).mapError(e => TamerError("Could not run tamer example", e))

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}

object MainGeneralized extends zio.App {
  val transactorLayer: Layer[TamerError, DbTransactor]                     = (Blocking.live ++ ConfigDb.live) >>> db.hikariLayer
  val kafkaLayer: Layer[TamerError, Kafka]                                 = Config.live >>> Kafka.live
  val queryConfigLayer: Layer[TamerError, DbConfig with QueryConfig]       = ConfigDb.live
  val myLayer: Layer[TamerError, DbTransactor with Kafka with QueryConfig] = transactorLayer ++ kafkaLayer ++ queryConfigLayer
  val program: ZIO[Kafka with TamerDBConfig with ZEnv, TamerError, Unit] = (for {
    log  <- log4sFromName.provide("tamer.example")
    _    <- log.info("Starting tamer...")
    boot <- UIO(Instant.now())
    earliest = boot.minus(60, DAYS)
    setup = tamer.db.mkSetup((s: MyState) =>
      sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${s.from} AND modified_at <= ${s.to}""".query[Value]
    )(
      defaultState = MyState(earliest, earliest.plus(5, MINUTES)),
      keyExtract = (value: Value) => Key(value.id),
      stateFoldM = (s: MyState) => {
        case QueryResult(_, results) if results.isEmpty => s.to.plus(5, MINUTES).orNow.map(MyState(s.from, _))
        case QueryResult(_, results) =>
          val mostRecent = results.sortBy(_.modifiedAt).max.instant
          mostRecent.plus(5, MINUTES).orNow.map(MyState(mostRecent, _))
      }
    )
    _ <- log.info(s"Tamer initialized with setup $setup")
    _ <- tamer.db.fetch(setup)
  } yield ()).mapError(e => TamerError("Could not run tamer example", e))

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.provideCustomLayer(myLayer).exitCode
}
