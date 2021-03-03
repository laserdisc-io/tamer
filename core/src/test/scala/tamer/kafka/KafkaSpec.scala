package tamer.kafka

import tamer.config.KafkaConfig
import tamer.kafka.KafkaTestUtils._
import tamer.kafka.embedded.KafkaTest
import tamer.{SourceConfiguration, TamerError}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{Console, putStrLn}
import zio.duration.durationInt
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test.environment.{TestConsole, TestEnvironment}
import zio.test.{DefaultRunnableSpec, TestFailure, ZSpec, assert}
import zio.{Chunk, Has, Queue, Ref, UIO, ZEnv, ZIO, ZLayer}

object KafkaSpec extends DefaultRunnableSpec {
  type OutputR = Ref[Vector[Int]]

  val kafkaLayer: ZLayer[Has[OutputR] with Console with KafkaConfig with Clock with Blocking, TamerError, Kafka] =
    Kafka.live(SourceConfiguration(SourceConfiguration.SourceSerde[Key, Value, State](), State(0), 0), stateTransitionFunction)

  val embeddedKafkaLayer: ZLayer[Has[OutputR] with ZEnv, Throwable, Kafka] = {
    val kafkaConfigLayer = KafkaTest.embeddedKafkaTest >>> KafkaTest.embeddedKafkaConfig
    kafkaConfigLayer ++ ZLayer.requires[ZEnv] ++ ZLayer.requires[Has[OutputR]] >>> kafkaLayer
  }

  val output: UIO[OutputR] = Ref.make(Vector.empty[Int])

  def stateTransitionFunction(s: State, q: Queue[Chunk[(Key, Value)]]): ZIO[Has[OutputR] with Console, TamerError, State] =
    ZIO.service[OutputR].flatMap { variable =>
      val cursor = s.i + 1
      if (cursor <= 10)
        variable.update(_.concat(Vector(cursor))) *>
          q.offer(Chunk((Key(cursor), Value(cursor)))).as(s.copy(i = cursor))
      else
        ZIO.never *> UIO(State(9999))
    }

  override def spec: ZSpec[TestEnvironment, Throwable] = {
    lazy val outputLayer = output.toLayer
    val tamerKafkaLayer: ZLayer[ZEnv, Throwable, Kafka] =
      (ZLayer.requires[ZEnv] ++ outputLayer) >>> embeddedKafkaLayer
    suite("KafkaSpec")(
      testM("should successfully run the stateTransitionFunction 10 times") {
        (for {
          outputVector <- ZIO.service[OutputR]
          _            <- tamer.kafka.runLoop.timeout(5.seconds)
          _            <- TestConsole.output.flatMap(s => putStrLn("bau: " + s.mkString))
          result       <- outputVector.get
        } yield assert(result)(equalTo(Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))))
      }
    )
      .provideSomeLayerShared[TestEnvironment](
        (KafkaTest.embeddedKafkaTest ++ tamerKafkaLayer ++ outputLayer)
          .mapError(TestFailure.fail)
      )
      .updateService[Clock.Service](_ => Clock.Service.live) @@ timeout(10.seconds)
  }
}
