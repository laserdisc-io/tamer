package tamer
package example

import tamer.config.{Config}
import tamer.db.Db.Datable
import tamer.db.{ConfigDb, DbTransactor, TamerDBConfig}
import tamer.kafka.Kafka
import zio.blocking.Blocking
import zio._
import doobie.implicits.legacy.instant._
import doobie.syntax.string._
import tamer.db.ConfigDb.{DbConfig, QueryConfig}

import java.time.temporal.ChronoUnit._
import java.time.{Duration, Instant}

final case class Key(id: String)
final case class Value(id: String, name: String, description: Option[String], modifiedAt: Instant) extends Datable(modifiedAt)
object Value {
  implicit val ordering: Ordering[Value] = (x: Value, y: Value) => x.modifiedAt.compareTo(y.modifiedAt)
}

object Main extends zio.App {
  val transactorLayer: Layer[TamerError, DbTransactor]                   = (Blocking.live ++ ConfigDb.live) >>> db.hikariLayer
  val kafkaLayer: Layer[TamerError, Kafka]                               = Config.live >>> Kafka.live
  val queryConfigLayer: Layer[TamerError, DbConfig with QueryConfig]     = ConfigDb.live
  val layer: Layer[TamerError, DbTransactor with Kafka with QueryConfig] = transactorLayer ++ kafkaLayer ++ queryConfigLayer
  def keyExtract(value: Value): Key                                      = Key(value.id)
  val program: ZIO[Kafka with TamerDBConfig with ZEnv, TamerError, Unit] = for {
    boot <- UIO(Instant.now())
    setup = tamer.db.mkSetup(ts =>
      sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${ts.from} AND modified_at <= ${ts.to}""".query[Value]
    )(
      earliest = boot.minus(60, DAYS),
      tumblingStep = Duration.of(5, MINUTES),
      keyExtract = keyExtract
    )
    _ <- tamer.db.fetchWithTimeSegment(setup)
  } yield ()

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.provideCustomLayer(layer).exitCode
}
