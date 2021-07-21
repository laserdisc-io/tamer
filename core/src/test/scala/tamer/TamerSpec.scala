package tamer

import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test.environment.TestEnvironment
import zio.test.{DefaultRunnableSpec, TestFailure, assert}

object TamerSpec extends DefaultRunnableSpec {
  case class Log(series: Vector[Int])
  object Log {
    val layer: ULayer[Has[Ref[Log]]] = Ref.make(Log(Vector.empty)).toLayer
  }

  val baseTamerLayer = Tamer.live {
    new Setup[Has[Ref[Log]], Key, Value, State] {
      override final val serdes       = Setup.Serdes[Key, Value, State]
      override final val initialState = State(0)
      override final val stateKey     = 0
      override final val recordKey    = (s: State, _: Value) => Key(s.state + 1)
      override final def iteration(s: State, q: Queue[Chunk[(Key, Value)]]): ZIO[Has[Ref[Log]], TamerError, State] =
        ZIO.service[Ref[Log]].flatMap { variable =>
          val cursor = s.state + 1
          if (cursor <= 10)
            variable.update(log => log.copy(log.series ++ Vector(cursor))) *>
              q.offer(Chunk((Key(cursor), Value(cursor)))).as(s.copy(state = cursor))
          else
            ZIO.never *> UIO(State(9999))
        }
    }
  }

  val embeddedKafkaTamerLayer =
    FakeKafka.embeddedKafkaConfigLayer ++ ZLayer.requires[Blocking with Clock] ++ Log.layer >>> baseTamerLayer

  override final val spec =
    suite("TamerSpec")(
      testM("should successfully run the iteration function 10 times") {
        for {
          outputVector <- ZIO.service[Ref[Log]]
          _            <- runLoop.timeout(7.seconds)
          result       <- outputVector.get
        } yield assert(result.series)(equalTo(Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
      } @@ timeout(20.seconds)
    ).provideSomeLayerShared[TestEnvironment]((embeddedKafkaTamerLayer ++ Log.layer).mapError(TestFailure.fail))
      .updateService[Clock.Service](_ => Clock.Service.live)
}
