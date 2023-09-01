package tamer

import org.apache.kafka.common.TopicPartition
import zio._
import zio.kafka.admin.AdminClient.{TopicPartition => AdminPartition}
import zio.test._
import zio.test.Assertion.equalTo
import zio.test.TestAspect.{timeout, withLiveClock}

object TamerSpec extends ZIOSpecDefault with TamerSpecGen {

  case class Log(series: Vector[Int])
  object Log {
    val layer: ULayer[Ref[Log]] = ZLayer(Ref.make(Log(Vector.empty)))
  }

  val baseTamerLayer = Tamer.live {
    new Setup[Ref[Log], Key, Value, State] {
      override final val serdes       = Setup.mkSerdes[Key, Value, State]
      override final val initialState = State(0)
      override final val stateKey     = 0
      override final val recordKey    = (s: State, _: Value) => Key(s.state + 1)
      override final def iteration(s: State, q: Enqueue[NonEmptyChunk[(Key, Value)]]): ZIO[Ref[Log], TamerError, State] =
        ZIO.service[Ref[Log]].flatMap { variable =>
          val cursor = s.state + 1
          if (cursor <= 10)
            variable.update(log => log.copy(log.series ++ Vector(cursor))) *>
              q.offer(NonEmptyChunk((Key(cursor), Value(cursor)))).as(s.copy(state = cursor))
          else ZIO.never *> ZIO.succeed(State(9999))
        }
    }
  }

  val embeddedKafkaTamerLayer = FakeKafka.embeddedKafkaConfigLayer ++ Log.layer >>> baseTamerLayer

  val p = new TopicPartition("a-test-topic", 1)

  override final val spec = suite("TamerSpec")(
    test("should successfully run the iteration function 10 times") {
      val x = for {
        outputVector <- ZIO.service[Ref[Log]]
        _            <- runLoop.timeout(7.seconds)
        result       <- outputVector.get
      } yield assert(result.series)(equalTo(Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
      x
    } @@ timeout(20.seconds)
  ).provideSomeLayerShared[TestEnvironment](embeddedKafkaTamerLayer ++ Log.layer) @@ withLiveClock

}

private[tamer] sealed trait TamerSpecGen {

  val testPartition      = new TopicPartition("gen-test-topic", 1)
  val testAdminPartition = AdminPartition(testPartition)

  private[this] val committedOffset = Gen.long(0L, 100L).map(l => Map(testPartition -> l))

  val offsets: Gen[Sized, (Map[TopicPartition, Long], Map[TopicPartition, Long])] =
    committedOffset.flatMap { co =>
      Gen.long(0L, 100L).map(lag => co -> Map(testPartition -> (co(testPartition) + lag)))
    }
}
