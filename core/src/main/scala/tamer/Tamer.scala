package tamer

import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.NonEmptyString
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{KafkaException, TopicPartition}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer._
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.stream.ZStream

final case class StateKey(stateKey: String, groupId: String)

trait Tamer {
  def runLoop: ZIO[Blocking with Clock, TamerError, Unit]
}

object Tamer {
  private val tenTimes = Schedule.recurs(10) && Schedule.exponential(100.milliseconds)

  private[this] final val tamerErrors: PartialFunction[Throwable, TamerError] = {
    case ke: KafkaException => TamerError(ke.getLocalizedMessage, ke)
    case te: TamerError     => te
  }

  final def sink[R, K, V](
      dataQueue: ZStream[Any, Nothing, (K, V)],
      valueProducerService: Producer.Service[R, K, V],
      sinkTopic: NonEmptyString,
      log: LogWriter[Task]
  ): RIO[R with Blocking with Clock, Unit] =
    dataQueue
      .map { case (k, v) => new ProducerRecord(sinkTopic, k, v) }
      .mapChunksM(recordChunk =>
        valueProducerService
          .produceChunkAsync(recordChunk)
          .tapError { _ =>
            log.info(s"Still cannot produce next chunk, $recordChunk")
          }
          .retry(tenTimes)
          .flatten <* log.info(s"pushed ${recordChunk.size} messages to $sinkTopic")
      )
      .runDrain
      .onError(e => log.warn(s"Could not push data to topic '$sinkTopic': ${e.prettyPrint}").orDie)

  case class Live[K, V, S](
      config: KafkaConfig,
      serdes: Setup.Serdes[K, V, S],
      defaultState: S,
      stateHash: Int,
      iteration: (S, Queue[Chunk[(K, V)]]) => Task[S],
      repr: String,
      stateConsumerService: Consumer.Service,
      stateProducerService: Producer.Service[RegistryInfo, StateKey, S],
      valueProducerService: Producer.Service[RegistryInfo, K, V]
  ) extends Tamer {
    private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.kafka")

    private final def mkRegistry(src: SchemaRegistryClient, topic: String): ULayer[RegistryInfo] =
      (ZLayer.succeed(src) >>> Registry.live) ++ ZLayer.succeed(topic)

    final def source(
        dataQueue: Queue[Chunk[(K, V)]],
        layer: ULayer[RegistryInfo],
        log: LogWriter[Task]
    ): ZStream[Blocking with Clock, Throwable, Offset] = {

      final case class TopicPartitionOffset(topic: String, partition: Int, offset: Long)

      // There are at least 3 possible decisions:
      //
      // 1. Set(partition offsets) == Set(0)                                       => initialize
      // 2. Set(partition offsets) &~ Set(committed partition offset) != Set.empty => resume
      // 3. Set(partition offsets) &~ Set(committed partition offset) == Set.empty => fail
      //
      // Notes:
      // Point 2 can also mean that the committed offsets are lagging behind more than 1.
      // This should not happen based on the assumption that a state will not be pushed
      // until the last batch has been put in the queue. Unfortunately, there's no such
      // guarantee currently, as there is no synchronization between the fiber that sends
      // the chunks of values to Kafka and the one that sends the new state to Kafka.
      // This should be addressed by https://github.com/laserdisc-io/tamer/issues/43
      // Ideally, we should use transactions for this but
      // a. zio-kafka does not support them yet (https://github.com/zio/zio-kafka/issues/33)
      // b. more importantly, they would not be able to span our strongly typed producers,
      //    so they would be "limited" to ensuring no state is visible to our consumer until
      //    the previous state offset has been committed.
      //
      // Point 3 is also totally arbitrary. If the consumer is current but there are (or have
      // been, depending how the state topic is configured) states in the topic the consumer
      // would stay put forever. So the decision here is to fail.
      sealed trait Decision  extends Product with Serializable
      case object Initialize extends Decision
      case object Resume     extends Decision
      case object Fail       extends Decision

      val stateKey = StateKey(stateHash.toHexString, config.state.groupId)
      def waitNonEmptyAssignment(consumerService: Consumer.Service) = consumerService.assignment
        .withFilter(_.nonEmpty)
        .tapError(_ => log.debug(s"still no assignment on ${config.state.groupId}, there are no partitions to process"))
        .retry(tenTimes)
      val stateTopicSub               = Subscription.topics(config.state.topic)
      def mkRecord(k: StateKey, v: S) = new ProducerRecord(config.state.topic, k, v)
      def decide(partitionSet: Set[TopicPartition]) = {
        val partitionOffsets = stateConsumerService.endOffsets(partitionSet).map {
          _.map { case (tp, o) => TopicPartitionOffset(tp.topic(), tp.partition(), o) }.toSet
        }
        val committedPartitionOffsets = stateConsumerService.committed(partitionSet).map {
          _.map {
            case (tp, Some(o)) => Some(TopicPartitionOffset(tp.topic(), tp.partition(), o.offset()))
            case _             => None
          }.flatten.toSet
        }
        partitionOffsets.zip(committedPartitionOffsets).map {
          case (po, _) if po.map(_.offset).forall(_ == 0L) => Initialize
          case (po, cpo) if po == cpo                      => Resume
          case _                                           => Fail
        }
      }

      val subscribeAndDecide = for {
        _          <- log.debug(s"subscribing to $stateTopicSub")
        _          <- stateConsumerService.subscribe(stateTopicSub)
        _          <- log.debug("awaiting assignment")
        assignment <- waitNonEmptyAssignment(stateConsumerService)
        _          <- log.debug(s"received assignment $assignment")
        decision   <- decide(assignment)
        _          <- log.debug(s"decided to $decision")
      } yield decision

      val stateKeySerde = Serde.value[StateKey]
      val maybeInitializeAssignedPartitions: ZIO[Blocking with Clock, Throwable, Unit] = subscribeAndDecide.flatMap {
        case Initialize =>
          log.info(s"consumer group ${config.state.groupId} never consumed from ${config.state.topic}") *>
            stateProducerService
              .produceAsync(mkRecord(stateKey, defaultState))
              .provideSomeLayer[Blocking](layer)
              .flatten
              .flatMap(recordMetadata => log.info(s"pushed initial state $defaultState to $recordMetadata"))
        case Resume => log.info(s"consumer group ${config.state.groupId} resuming consumption from ${config.state.topic}")
        case Fail =>
          log.error(s"consumer group ${config.state.groupId} is already at the end of ${config.state.topic}, manual intervention required") *>
            ZIO.fail(TamerError(s"Consumer group ${config.state.groupId} stuck at end of stream"))
      }

      val stateStream: ZStream[Blocking with Clock, Throwable, Offset] = stateConsumerService
        .plainStream(stateKeySerde.deserializer, serdes.stateSerde)
        .provideSomeLayer[Blocking with Clock](layer)
        .mapM {
          case CommittableRecord(record, offset) if record.key == stateKey =>
            log.debug(
              s"consumer group ${config.state.groupId} consumed state ${record.value} from ${offset.topicPartition}@${offset.offset}"
            ) *>
              iteration(record.value, dataQueue)
                .flatMap { newState =>
                  stateProducerService
                    .produceAsync(mkRecord(stateKey, newState))
                    .provideSomeLayer[Blocking](layer)
                    .flatten
                    .flatMap(rmd => log.debug(s"pushed state $newState to $rmd"))
                    .as(offset)
                }
          case CommittableRecord(record, offset) =>
            log.debug(
              s"consumer group ${config.state.groupId} ignored state (wrong key: ${record.key} != $stateKey) from ${offset.topicPartition}@${offset.offset}"
            ) *> UIO(offset)
        }

      ZStream.fromEffect(maybeInitializeAssignedPartitions).drain ++ stateStream
    }

    override final val runLoop: ZIO[Blocking with Clock, TamerError, Unit] = {
      def printSetup(logWriter: LogWriter[Task]): Task[Unit] = logWriter.info(s"initializing kafka loop with setup: \n$repr")

      (for {
        log                  <- logTask
        _                    <- printSetup(log)
        schemaRegistryClient <- Task(new CachedSchemaRegistryClient(config.schemaRegistryUrl, 4))
        chunkQueue           <- Queue.bounded[Chunk[(K, V)]](config.bufferSize)
        sinkRegistry  = mkRegistry(schemaRegistryClient, config.sink.topic)
        stateRegistry = mkRegistry(schemaRegistryClient, config.state.topic)
        queueStream   = ZStream.fromChunkQueueWithShutdown(chunkQueue)
        _ <- ZStream
          .fromEffect(sink(queueStream, valueProducerService, config.sink.topic, log).fork <* log.info("running sink perpetually"))
          .flatMap { fiber =>
            source(
              dataQueue = chunkQueue,
              layer = stateRegistry,
              log = log
            ).ensuringFirst {
              fiber.interrupt *> log
                .info("sink interrupted")
                .ignore *> sink(queueStream, valueProducerService, config.sink.topic, log).run <* log
                .info("sink queue drained")
                .ignore
            }
          }
          .aggregateAsync(Consumer.offsetBatches)
          .mapM(ob => ob.commit <* log.debug(s"consumer group ${config.state.groupId} committed offset batch ${ob.offsets}"))
          .provideSomeLayer[Blocking with Clock](sinkRegistry)
          .runDrain
      } yield ()).refineOrDie(tamerErrors)
    }
  }
  object Live {
    def getManaged[K, V, S](
        config: KafkaConfig,
        serdes: Setup.Serdes[K, V, S],
        defaultState: S,
        stateKey: Int,
        iteration: (S, Queue[Chunk[(K, V)]]) => Task[S],
        repr: String
    ): ZManaged[Clock with Blocking, TamerError, Live[K, V, S]] = {
      val offsetRetrievalStrategy = OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest)
      val cSettings = ConsumerSettings(config.brokers)
        .withProperties(config.properties)
        .withGroupId(config.state.groupId)
        .withClientId(config.state.clientId)
        .withCloseTimeout(config.closeTimeout.zio)
        .withOffsetRetrieval(offsetRetrievalStrategy)
      val pSettings = ProducerSettings(config.brokers)
        .withProperties(config.properties)
        .withCloseTimeout(config.closeTimeout.zio)

      val stateKeySerde = Serde.key[StateKey]
      val stateConsumer = Consumer.make(cSettings).mapError(t => TamerError("Could not make state consumer", t))
      val stateProducer = Producer
        .make(pSettings, stateKeySerde.serializer, serdes.stateSerde)
        .mapError(t => TamerError("Could not make state producer", t))
      val valueProducer = Producer
        .make(pSettings, serdes.keySerializer, serdes.valueSerializer)
        .mapError(t => TamerError("Could not make value producer", t))

      for {
        stateConsumerService <- stateConsumer
        stateProducerService <- stateProducer
        valueProducerService <- valueProducer
      } yield new Live(config, serdes, defaultState, stateKey, iteration, repr, stateConsumerService, stateProducerService, valueProducerService)
    }

    def getLayer[R, K, V, S](setup: Setup[R, K, V, S]): ZLayer[R with Clock with Blocking with Has[KafkaConfig], TamerError, Has[Tamer]] =
      ZLayer.fromServiceManaged[KafkaConfig, R with Clock with Blocking, TamerError, Tamer] { config =>
        val iteration = ZIO.environment[R].map(r => Function.untupled((setup.iteration _).tupled.andThen(_.provide(r)))).toManaged_
        iteration.flatMap(Live.getManaged(config, setup.serdes, setup.defaultState, setup.stateKey, _, setup.repr))
      }
  }

  def live[R, K, V, S](setup: Setup[R, K, V, S]): ZLayer[R with Has[KafkaConfig] with Clock with Blocking, TamerError, Has[Tamer]] =
    Live.getLayer(setup)
}
