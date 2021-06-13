package tamer

import zio.{Chunk, Has, Queue, Ref, UIO, ZEnv, ZIO, ZLayer}
import zio.clock.Clock
import zio.console.Console
import zio.duration.durationInt
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test.environment.TestEnvironment
import zio.test.{DefaultRunnableSpec, TestFailure, assert}

object KafkaSpec extends DefaultRunnableSpec {
  import TestUtils._
  type OutputR = Ref[Vector[Int]]

  val baseTamerLayer = Tamer.live {
    new Setup[Has[OutputR] with Console, Key, Value, State] {
      override final val serdes       = Setup.Serdes[Key, Value, State]
      override final val defaultState = State(0)
      override final val stateKey     = 0
      override final val recordKey    = (s: State, _: Value) => Key(s.state + 1)
      override final def iteration(s: State, q: Queue[Chunk[(Key, Value)]]): ZIO[Has[OutputR] with Console, TamerError, State] =
        ZIO.service[OutputR].flatMap { variable =>
          val cursor = s.state + 1
          if (cursor <= 10)
            variable.update(_ ++ Vector(cursor)) *>
              q.offer(Chunk((Key(cursor), Value(cursor)))).as(s.copy(state = cursor))
          else
            ZIO.never *> UIO(State(9999))
        }
    }
  }

  val embeddedKafkaTamerLayer = {
    val kafkaConfigLayer = FakeKafka.embedded >>> FakeKafka.embeddedKafkaConfig
    kafkaConfigLayer ++ ZLayer.requires[ZEnv] ++ ZLayer.requires[Has[OutputR]] >>> baseTamerLayer
  }

  val output = Ref.make(Vector.empty[Int])

  override final val spec = {
    lazy val outputLayer = output.toLayer
    val tamerLayer       = (ZLayer.requires[ZEnv] ++ outputLayer) >>> embeddedKafkaTamerLayer

    suite("TamerSpec")(
      testM("should successfully run the stateTransitionFunction 10 times") {
        (for {
          outputVector <- ZIO.service[OutputR]
          _            <- runLoop.timeout(7.seconds)
          result       <- outputVector.get
        } yield assert(result)(equalTo(Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))))
      } @@ timeout(20.seconds)
    )
      .provideSomeLayerShared[TestEnvironment](
        (FakeKafka.embedded ++ tamerLayer ++ outputLayer)
          .mapError(TestFailure.fail)
      )
      .updateService[Clock.Service](_ => Clock.Service.live)
  }
}
