package tamer

import io.confluent.kafka.schemaregistry.ParsedSchema
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatestplus.mockito.MockitoSugar.mock
import tamer.Tamer.StateKey
import utils.{FakeConsumer, FakeProducer}
import zio.clock.{Clock, sleep}
import zio.duration.Duration
import zio.kafka.admin.AdminClient
import zio.random.{Random, nextLongBounded}
import zio.test.Assertion._
import zio.test.TestAspect.nonFlaky
import zio.test._
import zio.{Chunk, Queue, Task, ZLayer}

object SourceSpec extends DefaultRunnableSpec {
  private[this] def nullRegistryInfoFor(topic: String) = ZLayer.succeed(topic) ++ ZLayer.succeed(new Registry {
    def getOrRegisterId(subject: String, schema: ParsedSchema): Task[Int] = ???
    def verifySchema(id: Int, schema: ParsedSchema): Task[Unit]           = ???
  })

  val setupSerdes: Setup.Serdes[Key, Value, State] = Setup.Serdes[Key, Value, State]
  override final val spec = suite("SourceSpec")(
//    testM("simple case") {
//      val data = for {
//        log       <- log4sFromName.provide("testSource.1")
//        records   <- Queue.unbounded[ProducerRecord[StateKey, State]]
//        producer  <- FakeProducer.mk[RegistryInfo, Tamer.StateKey, State](records, log)
//        consumer  <- FakeConsumer.mk(records, log)
//        dataQueue <- zio.Queue.unbounded[Chunk[(Key, Value)]]
//        _ <- Tamer
//          .source[Key, Value, State](
//            stateTopic = "topic",
//            stateGroupId = "topicGroupId",
//            stateHash = 0,
//            stateSerde = setupSerdes.stateSerde,
//            initialState = State(0),
//            stateConsumer = consumer,
//            stateProducer = producer,
//            kvChunkQueue = dataQueue,
//            registryLayer = nullRegistryInfoFor("topic"),
//            iterationFunction = (_: State, q: Queue[Chunk[(Key, Value)]]) => q.offer(Chunk((Key(1), Value(2)))) *> Task(State(0)),
//            log = log,
//            adminClient = mock[AdminClient],
//            stateRecovery = ManualRecovery
//          )
//          .take(1)
//          .runCollect
//      } yield dataQueue
//
//      assertM(data.flatMap(_.takeAll))(contains(Chunk((Key(1), Value(2)))))
//
//    },
//    testM("should point to a tape containing only State(3)") {
//      val tape = for {
//        log        <- log4sFromName.provide("testSource.2")
//        stateQueue <- Queue.unbounded[ProducerRecord[StateKey, State]]
//        producer   <- FakeProducer.mk[RegistryInfo, Tamer.StateKey, State](stateQueue, log)
//        consumer   <- FakeConsumer.mk(stateQueue, log)
//        dataQueue  <- zio.Queue.unbounded[Chunk[(Key, Value)]]
//
//        _ <- Tamer
//          .source[Key, Value, State](
//            stateTopic = "topic",
//            stateGroupId = "topicGroupId",
//            stateHash = 0,
//            stateSerde = setupSerdes.stateSerde,
//            initialState = State(0),
//            stateConsumer = consumer,
//            stateProducer = producer,
//            kvChunkQueue = dataQueue,
//            registryLayer = nullRegistryInfoFor("topic"),
//            iterationFunction = (s: State, q: Queue[Chunk[(Key, Value)]]) => {
//              val nextState = State(s.state + 1)
//              log.info(s"iteration function fakely computing $nextState as next state") *>
//                q.offer(Chunk((Key(0), Value(0)))) *>
//                Task(nextState)
//            },
//            log = log,
//            adminClient = mock[AdminClient],
//            stateRecovery = ManualRecovery
//          )
//          .take(3)
//          .runCollect
//      } yield stateQueue
//
//      assertM(tape.flatMap(_.takeAll.map(_.map(pr => pr.value()))))(equalTo(List(State(3))))
//    },
//    testM("should point to a tape containing only State(3) when resuming") {
//      val tape = for {
//        log        <- log4sFromName.provide("testSource.3")
//        stateQueue <- Queue.unbounded[ProducerRecord[StateKey, State]]
//        _          <- stateQueue.offer(new ProducerRecord("topic", StateKey("0", "topicGroupId"), State(2)))
//        producer   <- FakeProducer.mk[RegistryInfo, Tamer.StateKey, State](stateQueue, log)
//        consumer   <- FakeConsumer.mk(1, stateQueue, log)
//        dataQueue  <- zio.Queue.unbounded[Chunk[(Key, Value)]]
//        _ <- Tamer
//          .source[Key, Value, State](
//            stateTopic = "topic",
//            stateGroupId = "topicGroupId",
//            stateHash = 0,
//            stateSerde = setupSerdes.stateSerde,
//            initialState = State(0),
//            stateConsumer = consumer,
//            stateProducer = producer,
//            kvChunkQueue = dataQueue,
//            registryLayer = nullRegistryInfoFor("topic"),
//            iterationFunction = (s: State, q: Queue[Chunk[(Key, Value)]]) => {
//              val nextState = State(s.state + 1)
//              log.info(s"iteration function fakely computing $nextState as next state") *>
//                q.offer(Chunk((Key(0), Value(0)))) *>
//                Task(nextState)
//            },
//            log = log,
//            adminClient = mock[AdminClient],
//            stateRecovery = ManualRecovery
//          )
//          .take(1)
//          .runCollect
//      } yield stateQueue
//
//      assertM(tape.flatMap(_.takeAll.map(_.map(pr => pr.value()))))(equalTo(List(State(3))))
//    },
//    testM("when there are two non committed items in the tape, just crash") {
//      val tape = for {
//        log        <- log4sFromName.provide("testSource.4")
//        stateQueue <- Queue.unbounded[ProducerRecord[StateKey, State]]
//        _          <- stateQueue.offer(new ProducerRecord("topic", StateKey("0", "topicGroupId"), State(2)))
//        _          <- stateQueue.offer(new ProducerRecord("topic", StateKey("0", "topicGroupId"), State(3)))
//        producer   <- FakeProducer.mk[RegistryInfo, Tamer.StateKey, State](stateQueue, log)
//        consumer   <- FakeConsumer.mk(1, stateQueue, log)
//        dataQueue  <- zio.Queue.unbounded[Chunk[(Key, Value)]]
//        _ <- Tamer
//          .source[Key, Value, State](
//            stateTopic = "topic",
//            stateGroupId = "topicGroupId",
//            stateHash = 0,
//            stateSerde = setupSerdes.stateSerde,
//            initialState = State(0),
//            stateConsumer = consumer,
//            stateProducer = producer,
//            kvChunkQueue = dataQueue,
//            registryLayer = nullRegistryInfoFor("topic"),
//            iterationFunction = (s: State, q: Queue[Chunk[(Key, Value)]]) => {
//              val nextState = State(s.state + 1)
//              log.info(s"iteration function fakely computing $nextState as next state") *>
//                q.offer(Chunk((Key(0), Value(0)))) *>
//                Task(nextState)
//            },
//            log = log,
//            adminClient = mock[AdminClient],
//            stateRecovery = ManualRecovery
//          )
//          .take(1)
//          .runCollect
//      } yield stateQueue
//
//      assertM(tape.flatMap(_.takeAll.map(_.map(pr => pr.value()))).run)(fails(hasMessage(containsString("topicGroupId stuck at end of stream"))))
//    },
    testM("should point to a tape containing only State(3)") {
      for {
        randomId <- zio.random.nextIntBounded(10000)
        log        <- log4sFromName.provide(s"testSource.5.$randomId")
        stateQueue <- Queue.unbounded[ProducerRecord[StateKey, State]]
        producer   <- FakeProducer.mk[RegistryInfo, Tamer.StateKey, State](stateQueue, log)
        consumer   <- FakeConsumer.mk(stateQueue, log)
        dataQueue  <- zio.Queue.unbounded[Chunk[(Key, Value)]]
        randomShortDurationMillis <- nextLongBounded(500L)

        _ <- Tamer
          .source[Key, Value, State](
            stateTopic = "topic",
            stateGroupId = "topicGroupId",
            stateHash = 0,
            stateSerde = setupSerdes.stateSerde,
            initialState = State(0),
            stateConsumer = consumer,
            stateProducer = producer,
            kvChunkQueue = dataQueue,
            registryLayer = nullRegistryInfoFor("topic"),
            iterationFunction = (s: State, q: Queue[Chunk[(Key, Value)]]) => {
              val nextState = State(s.state + 1)
              log.info(s"iteration function fakely computing $nextState as next state") *>
                q.offer(Chunk((Key(0), Value(0)))) *>
                Task(nextState)
            },
            log = log,
            adminClient = mock[AdminClient],
            stateRecovery = ManualRecovery
          )
          .runDrain
          .fork
        _ <- sleep(Duration.fromMillis(randomShortDurationMillis))
        listOfNextStates <- stateQueue.takeAll.map(_.map(_.value()))
      } yield assert(listOfNextStates)(hasSize(equalTo(1)))
    }.provideSomeLayer[Clock with Random](Clock.live ++ Random.live) @@ nonFlaky
  )
}
