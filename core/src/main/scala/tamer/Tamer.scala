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
      setup: Setup[K, V, S],
      stateTransitionFunction: (S, Queue[Chunk[(K, V)]]) => ZIO[Any, TamerError, S],
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
      sealed trait ExistingState
      case object PreexistingState extends ExistingState
      case object EmptyState       extends ExistingState
      val stateKey = StateKey(setup.tamerStateKafkaRecordKey.toHexString, config.state.groupId)
      def waitNonemptyAssignment(consumerService: Consumer.Service) = consumerService.assignment
        .withFilter(_.nonEmpty)
        .tapError { _ =>
          log.info(s"Still no assignment on ${config.state.groupId}, there are no partitions to process")
        }
        .retry(tenTimes)
      val stateTopicSub               = Subscription.topics(config.state.topic)
      def mkRecord(k: StateKey, v: S) = new ProducerRecord(config.state.topic, k, v)
      def containsExistingState(partitionSet: Set[TopicPartition]) =
        stateConsumerService.endOffsets(partitionSet).map(_.values.exists(_ > 0L)).map(if (_) PreexistingState else EmptyState)

      val subscribeToExistingState = for {
        _          <- log.info(s"Will subscribe to $stateTopicSub")
        _          <- stateConsumerService.subscribe(stateTopicSub)
        _          <- log.info(s"Awaiting assignment")
        assignment <- waitNonemptyAssignment(stateConsumerService)
        _          <- log.info(s"Got assignment: $assignment")
        state      <- containsExistingState(assignment)
        _          <- log.info(s"Got state: $state")
      } yield state

      val stateKeySerde = Serde.value[StateKey]
      val initializeAssignedPartitions: ZIO[Blocking with Clock, Throwable, Unit] = subscribeToExistingState.flatMap {
        case PreexistingState => log.info(s"consumer group ${config.state.groupId} resuming consumption from '${config.state.topic}'")
        case EmptyState =>
          log.info(
            s"consumer group ${config.state.groupId} never consumed from ${config.state.topic}, setting offset to earliest"
          ) *> // TODO: check if the above statement is true: the assignment might be an empty partition from an otherwise nonempty topic?
            stateProducerService
              .produceAsync(mkRecord(stateKey, setup.defaultState))
              .provideSomeLayer[Blocking](layer)
              .flatten
              .flatMap(recordMetadata => log.info(s"pushed initial state ${setup.defaultState} to $recordMetadata"))
      }

      val interlockingStateStream: ZStream[Blocking with Clock, Throwable, Offset] = stateConsumerService
        .plainStream(stateKeySerde.deserializer, setup.serdes.stateSerde)
        .provideSomeLayer[Blocking with Clock](layer)
        .mapM {
          case CommittableRecord(record, offset) if record.key == stateKey =>
            log.debug(
              s"consumer group ${config.state.groupId} consumed state ${record.value} from ${offset.topicPartition}@${offset.offset}"
            ) *>
              stateTransitionFunction(record.value, dataQueue)
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

      ZStream.fromEffect(initializeAssignedPartitions).drain ++ interlockingStateStream
    }

    override final val runLoop: ZIO[Blocking with Clock, TamerError, Unit] = {
      def printSetup(logWriter: LogWriter[Task]): Task[Unit] = logWriter.info(s"initializing kafka loop with setup: \n${setup.repr}")

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
        setup: Setup[K, V, S],
        stateTransitionFunction: (S, Queue[Chunk[(K, V)]]) => ZIO[Any, TamerError, S]
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
        .make(pSettings, stateKeySerde.serializer, setup.serdes.stateSerde)
        .mapError(t => TamerError("Could not make state producer", t))
      val valueProducer = Producer
        .make(pSettings, setup.serdes.keySerializer, setup.serdes.valueSerializer)
        .mapError(t => TamerError("Could not make value producer", t))

      for {
        stateConsumerService <- stateConsumer
        stateProducerService <- stateProducer
        valueProducerService <- valueProducer
      } yield new Live(config, setup, stateTransitionFunction, stateConsumerService, stateProducerService, valueProducerService)
    }

    def getLayer[R, K, V, S](
        setup: Setup[K, V, S],
        stateTransitionFunction: (S, Queue[Chunk[(K, V)]]) => ZIO[R, TamerError, S]
    ): ZLayer[R with Clock with Blocking with Has[KafkaConfig], TamerError, Has[Tamer]] =
      ZLayer.fromServiceManaged[KafkaConfig, R with Clock with Blocking, TamerError, Tamer] { config =>
        val provider: URManaged[R, (S, Queue[Chunk[(K, V)]]) => IO[TamerError, S]] =
          ZIO.environment[R].map(r => Function.untupled(stateTransitionFunction.tupled.andThen(_.provide(r)))).toManaged_
        provider.flatMap(providedStateTransitionFunction => Live.getManaged(config, setup, providedStateTransitionFunction))
      }
  }

  def live[R, K, V, S](
      setup: Setup[K, V, S],
      stateTransitionFunction: (S, Queue[Chunk[(K, V)]]) => ZIO[R, TamerError, S]
  ): ZLayer[R with Has[KafkaConfig] with Clock with Blocking, TamerError, Has[Tamer]] = Live.getLayer(setup, stateTransitionFunction)
}
