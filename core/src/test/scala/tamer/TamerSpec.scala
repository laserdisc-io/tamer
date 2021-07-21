package tamer

import org.apache.kafka.common.TopicPartition
import tamer.Tamer.{Die, Initialize, Recover, Resume}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.kafka.admin.AdminClient.{TopicPartition => AdminPartition}
import zio.random.Random
import zio.test.Assertion.{equalTo, isFalse}
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

  val p = new TopicPartition("a-test-topic", 1)

  override final val spec =
    suite("TamerSpec")(
      test("should successfully fix the state consumer's offset when the lag is greater than one") {
        val lags             = Map(p -> 3L)
        val brokenPartitions = Map(p -> 10L)
        val fixedPartitions  = Tamer.stateOffsetCorrection(lags, brokenPartitions)

        val p1Offset = fixedPartitions(AdminPartition(p))

        assert(p1Offset.offset)(equalTo(12L))
        assert(p1Offset.metadata)(equalTo(Some("Tamer offset correction. Topic: a-test-topic. Partition: 1. Lag was 3, offset corrected to 12")))
      },
      test("should successfully fix the state consumer's offset when the lag is 0") {
        val lags             = Map(p -> 0L)
        val brokenPartitions = Map(p -> 7L)
        val fixedPartitions  = Tamer.stateOffsetCorrection(lags, brokenPartitions)

        val p1Offset = fixedPartitions(AdminPartition(p))

        assert(p1Offset.offset)(equalTo(6L))
        assert(p1Offset.metadata)(equalTo(Some("Tamer offset correction. Topic: a-test-topic. Partition: 1. Lag was 0, offset corrected to 6")))
      },
      test("should ignore the state consumer's offset when the lag is 1") {
        val lags             = Map(p -> 1L)
        val brokenPartitions = Map(p -> 3L)
        val fixedPartitions  = Tamer.stateOffsetCorrection(lags, brokenPartitions)

        assert(fixedPartitions.contains(AdminPartition(p)))(isFalse)
      },
      testM("should deduce correctly the startup action from the offsets when recovery is automatic") {
        check(offsets) { case (committed, end) =>
          val action = Tamer.decidedAction(AutomaticRecovery)(end, committed)
          action match {
            case Initialize          => assert(end(testPartition))(equalTo(0L))
            case Resume              => assert(end(testPartition) - committed(testPartition))(equalTo(1L))
            case Recover(correction) => assert(end(testPartition) - correction(testAdminPartition).offset)(equalTo(1L))
            case Die                 => throw new Exception("Should never happen with AutomaticRecovery strategy")
          }
        }
      },
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
