package tamer

import io.confluent.kafka.schemaregistry.ParsedSchema
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatestplus.mockito.MockitoSugar.mock
import tamer.Tamer.StateKey
import utils.{FakeConsumer, FakeProducer}
import zio.clock.{Clock, sleep}
import zio.duration.Duration
import zio.kafka.admin.AdminClient
import zio.random.{Random, nextLongBetween}
import zio.test.Assertion._
import zio.test.TestAspect.{failing, nonFlaky}
import zio.test._
import zio.{Chunk, Queue, Task, ZLayer}

object SourceSpec extends DefaultRunnableSpec {
  private[this] def nullRegistryInfoFor(topic: String) = ZLayer.succeed(topic) ++ ZLayer.succeed(new Registry {
    def getOrRegisterId(subject: String, schema: ParsedSchema): Task[Int] = ???
    def verifySchema(id: Int, schema: ParsedSchema): Task[Unit]           = ???
  })

  val setupSerdes: Setup.Serdes[Key, Value, State] = Setup.Serdes[Key, Value, State]
  override final val spec = suite("SourceSpec")(
    testM("check that data is present") {
      val data = for {
        log       <- log4sFromName.provide("testSource.1")
        records   <- Queue.unbounded[ProducerRecord[StateKey, State]]
        producer  <- FakeProducer.mk[RegistryInfo, Tamer.StateKey, State](records, log)
        consumer  <- FakeConsumer.mk(records, log)
        dataQueue <- zio.Queue.unbounded[Chunk[(Key, Value)]]
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
            iterationFunction = (_: State, q: Queue[Chunk[(Key, Value)]]) => q.offer(Chunk((Key(1), Value(2)))) *> Task(State(0)),
            log = log,
            adminClient = mock[AdminClient],
            stateRecovery = ManualRecovery
          )
          .take(1)
          .runCollect
      } yield dataQueue

      assertM(data.flatMap(_.takeAll))(contains(Chunk((Key(1), Value(2)))))

    },
    testM("should point to a tape containing only State(3)") {
      val tape = for {
        log        <- log4sFromName.provide("testSource.2")
        stateQueue <- Queue.unbounded[ProducerRecord[StateKey, State]]
        producer   <- FakeProducer.mk[RegistryInfo, Tamer.StateKey, State](stateQueue, log)
        inFlight  <- zio.Queue.unbounded[(TopicPartition, ProducerRecord[StateKey, State])]
        consumer   <- FakeConsumer.mk(stateQueue, inFlight, log)
        dataQueue  <- zio.Queue.unbounded[Chunk[(Key, Value)]]

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
          .take(3)
          .runCollect
      } yield stateQueue

      assertM(tape.flatMap(_.takeAll.map(_.map(pr => pr.value()))))(equalTo(List(State(3))))
    },
    testM("should point to a tape containing only State(3) when resuming") {
      val tape = for {
        log        <- log4sFromName.provide("testSource.3")
        stateQueue <- Queue.unbounded[ProducerRecord[StateKey, State]]
        _          <- stateQueue.offer(new ProducerRecord("topic", StateKey("0", "topicGroupId"), State(2)))
        producer   <- FakeProducer.mk[RegistryInfo, Tamer.StateKey, State](stateQueue, log)
        consumer   <- FakeConsumer.mk(1, stateQueue, log)
        dataQueue  <- zio.Queue.unbounded[Chunk[(Key, Value)]]
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
          .take(1)
          .runCollect
      } yield stateQueue

      assertM(tape.flatMap(_.takeAll.map(_.map(pr => pr.value()))))(equalTo(List(State(3))))
    },
    testM("when there are two non committed items in the tape, just crash") {
      val tape = for {
        log        <- log4sFromName.provide("testSource.4")
        stateQueue <- Queue.unbounded[ProducerRecord[StateKey, State]]
        _          <- stateQueue.offer(new ProducerRecord("topic", StateKey("0", "topicGroupId"), State(2)))
        _          <- stateQueue.offer(new ProducerRecord("topic", StateKey("0", "topicGroupId"), State(3)))
        producer   <- FakeProducer.mk[RegistryInfo, Tamer.StateKey, State](stateQueue, log)
        consumer   <- FakeConsumer.mk(1, stateQueue, log)
        dataQueue  <- zio.Queue.unbounded[Chunk[(Key, Value)]]
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
          .take(1)
          .runCollect
      } yield stateQueue

      assertM(tape.flatMap(_.takeAll.map(_.map(pr => pr.value()))).run)(fails(hasMessage(containsString("topicGroupId stuck at end of stream"))))
    },
    testM("tape should not have more than one state") {
      for {
        randomId <- zio.random.nextIntBounded(10000)
        log        <- log4sFromName.provide(s"testSource.5.$randomId")
        stateQueue <- Queue.unbounded[ProducerRecord[StateKey, State]]
        producer   <- FakeProducer.mk[RegistryInfo, Tamer.StateKey, State](stateQueue, log)
        inFlight <- Queue.unbounded[(TopicPartition, ProducerRecord[StateKey, State])]
        consumer   <- FakeConsumer.mk(stateQueue, inFlight, log)
        dataQueue  <- zio.Queue.unbounded[Chunk[(Key, Value)]]
        randomShortDurationMillis <- nextLongBetween(50L, 100L)

        sourceFiber <- Tamer
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
        _ <- sourceFiber.interrupt
        listOfNextStates <- stateQueue.takeAll.map(_.map(_.value()))
        listOfInFlight <- inFlight.takeAll.map(_.map(_._2.value()))
        tapeSize = listOfNextStates.size + listOfInFlight.size
        data <- dataQueue.takeAll
        sourceHasStartedWorking = assert(data)(isEmpty).negate

      } yield sourceHasStartedWorking ==> assert(tapeSize)(isLessThanEqualTo(1))
    }.provideSomeLayer[Clock with Random](Clock.live ++ Random.live) @@ nonFlaky @@ failing,
    // TODO: the above test is marked as `failing` because this is not our target state, we would like that,
    // whenever Tamer crashes, there is always exactly one uncommitted message in the state topic.
    // This test was written mainly to characterize this behaviour as long as we are using async API so that
    // other test may be written with high fidelity to the real behaviour.
    testM("tape should always have at least 1 state") {
      for {
        randomId <- zio.random.nextIntBounded(10000)
        log        <- log4sFromName.provide(s"testSource.6.$randomId")
        stateQueue <- Queue.unbounded[ProducerRecord[StateKey, State]]
        producer   <- FakeProducer.mk[RegistryInfo, Tamer.StateKey, State](stateQueue, log)
        inFlight <- Queue.unbounded[(TopicPartition, ProducerRecord[StateKey, State])]
        consumer   <- FakeConsumer.mk(stateQueue, inFlight, log)
        dataQueue  <- zio.Queue.unbounded[Chunk[(Key, Value)]]
        randomShortDurationMillis <- nextLongBetween(50L, 100L)

        sourceFiber <- Tamer
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
        _ <- sourceFiber.interrupt
        listOfNextStates <- stateQueue.takeAll.map(_.map(_.value()))
        listOfInFlight <- inFlight.takeAll.map(_.map(_._2.value()))
        tapeSize = listOfNextStates.size + listOfInFlight.size
        data <- dataQueue.takeAll
        sourceHasStartedWorking = assert(data)(isEmpty).negate

      } yield sourceHasStartedWorking ==> assert(tapeSize)(isGreaterThanEqualTo(1))
    }.provideSomeLayer[Clock with Random](Clock.live ++ Random.live) @@ nonFlaky @@ failing
    // TODO: whenever there is a lag between the commit of the old state and the publishing
    // of the next state this test should fail. This is not desirable of course, the only
    // purpose of this test is to correctly characterize the fake kafka so that further test
    // can be devised with high fidelity.
  ) // you can add `@@ sequential` here as a trick to order the logs
}
