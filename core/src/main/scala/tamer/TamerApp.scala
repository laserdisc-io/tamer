package tamer

import tamer.config._
import tamer.db.{Db, DbTransactor}
import tamer.kafka.Kafka
import zio.{App, ExitCode, IO, Layer, URIO, ZEnv}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console._

abstract class TamerApp[K, V, State](private val setup: IO[TamerError, Setup[K, V, State]]) extends App {
  final val run = for {
    setup      <- setup
    program    <- kafka.runLoop(setup)(db.runQuery(setup))
  } yield program

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val transactorLayer: Layer[TamerError, DbTransactor] = (Blocking.live ++ Config.live) >>> Db.hikariLayer
    val dbLayer: Layer[TamerError, Db]                   = (Config.live ++ transactorLayer) >>> Db.live
    val kafkaLayer: Layer[TamerError, Kafka]             = Config.live >>> Kafka.live
    run
      .provideLayer(Blocking.live ++ Clock.live ++ dbLayer ++ kafkaLayer)
      .foldM(
        err => putStrLn(s"Execution failed with: $err") *> IO.succeed(ExitCode.failure),
        _ => IO.succeed(ExitCode.success)
      )
  }
}
