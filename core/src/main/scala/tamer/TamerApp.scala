package tamer

import tamer.config.Config
import tamer.db.Db
import tamer.kafka.Kafka
import zio._
import zio.blocking.Blocking
import zio.clock.Clock

abstract class TamerApp[K, V, State](setupR: UIO[Setup[K, V, State]]) extends App {
  final val run: ZIO[Blocking with Clock with Config with Db with Kafka, TamerError, Unit] =
    for {
      setup       <- setupR
      conf        <- config.load
      blockingEC  <- blocking.blockingExecutor.map(_.asEC)
      transactorR = Db.mkTransactor(conf.db, platform.executor.asEC, blockingEC)
      program <- transactorR.use { tnx =>
                  kafka.run(conf.kafka, setup) { case (sv, queue) => db.runQuery(tnx, setup.buildQuery(sv), queue, setup.valueToKey) }
                }
    } yield program

  override final def run(args: List[String]): ZIO[ZEnv, Nothing, Int] =
    run
      .provide(new Blocking.Live with Clock.Live with Config.Live with Db.Live with Kafka.Live {})
      .foldM(
        err => console.putStrLn(s"Execution failed with: $err") *> IO.succeed(1),
        _ => IO.succeed(0)
      )
}
