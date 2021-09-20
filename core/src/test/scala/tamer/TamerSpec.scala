package tamer

import org.apache.kafka.common.TopicPartition
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.kafka.admin.AdminClient.{TopicPartition => AdminPartition}
import zio.random.Random
import zio.test.Assertion.equalTo
import zio.test.TestAspect.timeout
import zio.test._
import zio.test.environment.TestEnvironment

object TamerSpec extends DefaultRunnableSpec with TamerSpecGen {

  case class Log(series: Vector[Int])
  object Log {
    val layer: ULayer[Has[Ref[Log]]] = Ref.make(Log(Vector.empty)).toLayer
  }

  val baseTamerLayer = Tamer.live {
    new Setup[Has[Ref[Log]], Key, Value, State] {
      override final val serdes       = Setup.mkSerdes[Key, Value, State]
      override final val initialState = State(0)
      override final val stateKey     = 0
      override final val recordKey    = (s: State, _: Value) => Key(s.state + 1)
      override final def iteration(s: State, q: Enqueue[Chunk[(Key, Value)]]): ZIO[Has[Ref[Log]], TamerError, State] =
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

  val p = new TopicPartition("a-test-topic", 1)

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

private[tamer] sealed trait TamerSpecGen {

  val testPartition      = new TopicPartition("gen-test-topic", 1)
  val testAdminPartition = AdminPartition(testPartition)

  private val committedOffset: Gen[Random with Sized, Map[TopicPartition, Long]] =
    Gen.long(0L, 100L).map(l => Map(testPartition -> l))

  val offsets: Gen[Random with Sized, (Map[TopicPartition, Long], Map[TopicPartition, Long])] =
    committedOffset.flatMap { co =>
      Gen.long(0L, 100L).map(lag => co -> Map(testPartition -> (co(testPartition) + lag)))
    }
}
