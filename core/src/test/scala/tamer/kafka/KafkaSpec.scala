package tamer.kafka

import tamer.config.KafkaConfig
import tamer.kafka.KafkaTestUtils._
import tamer.kafka.embedded.KafkaTest
import tamer.{SourceConfiguration, TamerError}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{Console, putStrLn}
import zio.duration.durationInt
import zio.kafka.consumer.{Consumer, Subscription}
import zio.kafka.serde.Serde
import zio.test.Assertion.isNonEmpty
import zio.test.TestAspect.timeout
import zio.test.environment.{TestConsole, TestEnvironment}
import zio.test.{DefaultRunnableSpec, TestFailure, ZSpec, assert}
import zio.{Chunk, Queue, UIO, ZEnv, ZIO, ZLayer}

object KafkaSpec extends DefaultRunnableSpec {
  val kafkaLayer: ZLayer[Console with KafkaConfig with Clock with Blocking, TamerError, Kafka] =
    Kafka.live(SourceConfiguration(SourceConfiguration.SourceSerde[Key, Value, State](), State(0), 0), stf)

  val embeddedKafkaLayer: ZLayer[ZEnv, Throwable, Kafka] = {
    val kafkaConfigLayer: ZLayer[Any, Throwable, KafkaConfig] = KafkaTest.embeddedKafkaTest >>> KafkaTest.embeddedKafkaConfig
    (kafkaConfigLayer ++ TestEnvironment.live) >>> kafkaLayer
  }

  def stf(s: State, q: Queue[Chunk[(Key, Value)]]): ZIO[Console, TamerError, State] = {
    val cursor = s.i + 1
    if (cursor < 10)
      putStrLn(s"cursor to $cursor") *> q.offer(Chunk((Key(cursor), Value(cursor)))).as(s.copy(i = cursor))
    else
      ZIO.never *> UIO(State(9999))
  }
  override def spec: ZSpec[TestEnvironment, Throwable] =
    suite("KafkaSpec")(
      testM("should work in an embedded environment") {
        for {
          _ <- produceMany("sink.topic", List(("ciao", "valore")))
          _ <- Consumer
            .subscribeAnd(Subscription.Topics(Set("sink.topic")))
            .plainStream(Serde.string, Serde.string)
            .take(1)
            .runLast
            .provideSomeLayer[KafkaTest with Blocking with Clock](consumer("embedded.groupid", "embedded.clientid"))
          _   <- tamer.kafka.runLoop.timeout(7.seconds)
          out <- TestConsole.output
        } yield assert(out)(isNonEmpty)
      }
    ).provideSomeLayerShared[TestEnvironment](
      ((KafkaTest.embeddedKafkaTest >>> stringProducer) ++ KafkaTest.embeddedKafkaTest ++ embeddedKafkaLayer)
        .mapError(TestFailure.fail) ++ Clock.live
    ) @@ timeout(10.seconds)
}
