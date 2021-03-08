package tamer.example

import doobie.syntax.string._
import tamer.config.{Config, KafkaConfig}
import tamer.db.ConfigDb.{DbConfig, QueryConfig}
import tamer.db.{ConfigDb, DbTransactor, DoobieConfiguration, InstantOps, QueryResult, TamerDBConfig}
import tamer.{AvroCodec, HashableState, TamerError, db}
import zio._
import zio.blocking.Blocking

import java.time.Instant
import java.time.temporal.ChronoUnit._
import scala.util.hashing.byteswap64

final case class MyState(from: Instant, to: Instant)

object MyState {
  implicit val codec = AvroCodec.codec[MyState]

  implicit object MyStateHashable extends HashableState[MyState] {
    override def stateHash(s: MyState): Int = (byteswap64(s.from.getEpochSecond) + byteswap64(s.to.getEpochSecond)).intValue
  }

}

object DatabaseGeneralized extends zio.App {
  lazy val transactorLayer: Layer[TamerError, DbTransactor]               = (Blocking.live ++ ConfigDb.live) >>> db.hikariLayer
  lazy val queryConfigLayer: Layer[TamerError, DbConfig with QueryConfig] = ConfigDb.live
  lazy val kafkaConfig: Layer[TamerError, KafkaConfig]                    = Config.live
  lazy val myLayer: Layer[TamerError, DbTransactor with DbConfig with QueryConfig with KafkaConfig] =
    transactorLayer ++ queryConfigLayer ++ kafkaConfig
  lazy val program: ZIO[ZEnv with DbConfig with TamerDBConfig with KafkaConfig, TamerError, Unit] = (for {
    boot <- UIO(Instant.now())
    earliest = boot.minus(60, DAYS)
    setup = DoobieConfiguration(query)(
      defaultState = MyState(earliest, earliest.plus(5, MINUTES)),
      keyExtract = (value: Value) => Key(value.id),
      stateFoldM = (s: MyState) => {
        case QueryResult(_, results) if results.isEmpty => s.to.plus(5, MINUTES).orNow.map(MyState(s.from, _))
        case QueryResult(_, results) =>
          val mostRecent = results.sortBy(_.modifiedAt).max.timestamp
          mostRecent.plus(5, MINUTES).orNow.map(MyState(mostRecent, _))
      }
    )
    _ <- tamer.db.TamerDoobieJob(setup).fetch()
  } yield ()).mapError(e => TamerError("Could not run tamer example", e))

  private def query(s: MyState): doobie.Query0[Value] = {
    import doobie.implicits.legacy.instant._
    sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${s.from} AND modified_at <= ${s.to}""".query[Value]
  }

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.provideCustomLayer(myLayer).exitCode
}
