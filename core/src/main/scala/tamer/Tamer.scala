package tamer

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
import zio.stream.{UStream, ZStream}

trait Tamer {
  def runLoop: ZIO[Blocking with Clock, TamerError, Unit]
}

object Tamer {
  private[this] final case class StateKey(stateKey: String, groupId: String)
  private[this] final val stateKeySerde = Serde.key[StateKey]

  private[this] final case class TopicPartitionOffset(topic: String, partition: Int, offset: Long)

  private[this] final val tenTimes = Schedule.recurs(10) && Schedule.exponential(100.milliseconds) //FIXME make configurable

  private[this] final val tamerErrors: PartialFunction[Throwable, TamerError] = {
    case ke: KafkaException => TamerError(ke.getLocalizedMessage, ke)
    case te: TamerError     => te
  }

  private[this] final def mkRegistry(schemaRegistryClient: SchemaRegistryClient, topic: String): ULayer[RegistryInfo] =
    ZLayer.succeed(schemaRegistryClient) >>> Registry.live ++ ZLayer.succeed(topic)

  private[tamer] final def sink[R, K, V](kvStream: UStream[(K, V)], producer: Producer.Service[R, K, V], topic: String, log: LogWriter[Task]) =
    kvStream
      .map { case (k, v) => new ProducerRecord(topic, k, v) }
      .mapChunksM { recordChunk =>
        producer
          .produceChunkAsync(recordChunk)
          .tapError(_ => log.debug(s"failed pushing ${recordChunk.size} messages to $topic"))
          .retry(tenTimes)
          .flatten <* log.info(s"pushed ${recordChunk.size} messages to $topic")
      }
      .runDrain
      .onError(e => log.warn(s"could not push to topic $topic: ${e.prettyPrint}").orDie)

  private[tamer] final def source[K, V, S](
      stateTopic: String,
      stateGroupId: String,
      stateHash: Int,
      stateSerde: ZSerde[RegistryInfo, S],
      initialState: S,
      stateConsumer: Consumer.Service,
      stateProducer: Producer.Service[RegistryInfo, StateKey, S],
      kvChunkQueue: Queue[Chunk[(K, V)]],
      registryLayer: ULayer[RegistryInfo],
      iterationFunction: (S, Queue[Chunk[(K, V)]]) => Task[S],
      log: LogWriter[Task]
  ) = {

    val key          = StateKey(stateHash.toHexString, stateGroupId)
    val subscription = Subscription.topics(stateTopic)

    // There are at least 3 possible decisions:
    //
    // 1. Set(partition offsets) == Set(0L)                                      => initialize
    // 2. (Set(partition offsets) &~ Set(committed partition offset)).size == 1  => resume
    // 3. (Set(partition offsets) &~ Set(committed partition offset)) == Set()   => retry
    // 4. _                                                                      => fail
    //
    // Notes:
    // Point 2 should be further refined, to ensure that the partition offset that is
    // not in the committed set is also > than the maximum committed offset for the same.
    // Point 3 is totally arbitrary, and currently implemented in the same manner as point
    // 4. If the consumer is current but there are (or have been, depending how the state
    // topic is configured) states in the topic the consumer would stay put forever. The
    // decision here is to retry, that is shift back offset by 1.
    // Point 4 is the catch all. Take the case when the committed offsets are lagging
    // behind more than 1. This should not happen based on the assumption that a state
    // will not be pushed until the last batch has been put in the queue. Unfortunately,
    // there's no such guarantee currently, as there is no synchronization between the
    // fiber that sends the chunks of values to Kafka and the one that sends the new state
    // to Kafka. This should be addressed by https://github.com/laserdisc-io/tamer/issues/43
    // Ideally, we should use transactions for this but
    // a. zio-kafka does not support them yet (https://github.com/zio/zio-kafka/issues/33)
    // b. more importantly, they would not be able to span our strongly typed producers,
    //    so they would be "limited" to ensuring no state is visible to our consumer until
    //    the previous state offset has been committed.
    sealed trait Decision  extends Product with Serializable
    case object Initialize extends Decision
    case object Resume     extends Decision
    case object Retry      extends Decision
    case object Fail       extends Decision

    val waitForAssignment = stateConsumer.assignment
      .withFilter(_.nonEmpty)
      .tapError(_ => log.debug(s"still no assignment on $stateGroupId, there are no partitions to process"))
      .retry(tenTimes)

    val decide = (partitionSet: Set[TopicPartition]) => {
      val partitionOffsets = stateConsumer.endOffsets(partitionSet).map {
        _.map { case (tp, o) => TopicPartitionOffset(tp.topic(), tp.partition(), o) }.toSet
      }
      val committedPartitionOffsets = stateConsumer.committed(partitionSet).map {
        _.map {
          case (tp, Some(o)) => Some(TopicPartitionOffset(tp.topic(), tp.partition(), o.offset()))
          case _             => None
        }.flatten.toSet
      }
      partitionOffsets.zip(committedPartitionOffsets).map {
        case (po, _) if po.map(_.offset) == Set(0L) => Initialize
        case (po, cpo) if (po diff cpo).size == 1   => Resume
        case (po, cpo) if (po diff cpo) == Set()    => Retry
        case _                                      => Fail
      }
    }

    val subscribeAndDecide = log.debug(s"subscribing to $subscription") *>
      stateConsumer.subscribe(subscription) *>
      log.debug("awaiting assignment") *>
      waitForAssignment.flatMap(a => log.debug(s"received assignment $a") *> decide(a).tap(d => log.debug(s"decided to $d")))

    val initialize = subscribeAndDecide.flatMap {
      case Initialize =>
        log.info(s"consumer group $stateGroupId never consumed from $stateTopic") *>
          stateProducer
            .produceAsync(stateTopic, key, initialState)
            .provideSomeLayer[Blocking](registryLayer)
            .flatten
            .flatMap(rmd => log.info(s"pushed initial state $initialState to $stateTopic-${rmd.partition()}@${rmd.offset()}"))
      case Resume => log.info(s"consumer group $stateGroupId resuming consumption from $stateTopic")
      case Retry | Fail => // TODO: fix handling of retry case
        log.error(s"consumer group $stateGroupId is already at the end of $stateTopic, manual intervention required") *>
          ZIO.fail(TamerError(s"Consumer group $stateGroupId stuck at end of stream"))
    }

    val stateStream = stateConsumer
      .plainStream(stateKeySerde.deserializer, stateSerde)
      .provideSomeLayer[Blocking with Clock](registryLayer)
      .mapM {
        case CommittableRecord(record, offset) if record.key == key =>
          log.debug(s"consumer group $stateGroupId consumed state ${record.value} from ${offset.topicPartition}@${offset.offset}") *>
            // we got a valid state to process, invoke the handler
            iterationFunction(record.value, kvChunkQueue)
              .flatMap { newState =>
                // now the handler has concluded its job, we've a new state to save to Kafka
                stateProducer
                  .produceAsync(stateTopic, key, newState)
                  .provideSomeLayer[Blocking](registryLayer)
                  .flatten
                  .flatMap(rmd => log.debug(s"pushed state $newState to $rmd"))
                  .as(offset)
              }
        case CommittableRecord(record, offset) =>
          log.debug(
            s"consumer group $stateGroupId ignored state (wrong key: ${record.key} != $key) from ${offset.topicPartition}@${offset.offset}"
          ) *>
            UIO(offset)
      }

    ZStream.fromEffect(initialize).drain ++ stateStream
  }

  final class Live[K, V, S](
      config: KafkaConfig,
      serdes: Setup.Serdes[K, V, S],
      initialState: S,
      stateHash: Int,
      iterationFunction: (S, Queue[Chunk[(K, V)]]) => Task[S],
      repr: String,
      stateConsumer: Consumer.Service,
      stateProducer: Producer.Service[RegistryInfo, StateKey, S],
      valueProducer: Producer.Service[RegistryInfo, K, V]
  ) extends Tamer {

    private[this] final val logTask = log4sFromName.provide("tamer.kafka")

    private[this] final val SinkConfig(sinkTopic)                    = config.sink
    private[this] final val StateConfig(stateTopic, stateGroupId, _) = config.state

    private[tamer] final def sinkStream(kvStream: UStream[(K, V)], log: LogWriter[Task]) =
      ZStream.fromEffect(sink(kvStream, valueProducer, sinkTopic, log).fork <* log.info("running sink perpetually"))

    private[tamer] final def sourceStream(schemaRegistryClient: SchemaRegistryClient, kvChunkQueue: Queue[Chunk[(K, V)]], log: LogWriter[Task]) =
      source(
        stateTopic,
        stateGroupId,
        stateHash,
        serdes.stateSerde,
        initialState,
        stateConsumer,
        stateProducer,
        kvChunkQueue,
        mkRegistry(schemaRegistryClient, stateTopic),
        iterationFunction,
        log
      )

    private[tamer] final def drainSink(kvStream: UStream[(K, V)], log: LogWriter[Task]) =
      log.info("sink interrupted").ignore *>
        sink(kvStream, valueProducer, sinkTopic, log).run <*
        log.info("sink queue drained").ignore

    private[tamer] final def runLoop(
        schemaRegistryClient: SchemaRegistryClient,
        kvChunkQueue: Queue[Chunk[(K, V)]],
        kvStream: UStream[(K, V)],
        log: LogWriter[Task]
    ) = sinkStream(kvStream, log)
      .flatMap(fiber => sourceStream(schemaRegistryClient, kvChunkQueue, log).ensuringFirst(fiber.interrupt *> drainSink(kvStream, log)))
      .aggregateAsync(Consumer.offsetBatches)
      .mapM(ob => ob.commit <* log.debug(s"consumer group $stateGroupId committed offset batch ${ob.offsets}"))
      .provideSomeLayer[Blocking with Clock](mkRegistry(schemaRegistryClient, sinkTopic))

    override final val runLoop: ZIO[Blocking with Clock, TamerError, Unit] = {
      val logic = for {
        log                  <- logTask
        _                    <- log.info(s"initializing Tamer with setup: \n$repr")
        schemaRegistryClient <- Task(new CachedSchemaRegistryClient(config.schemaRegistryUrl, 1000)) // FIXME magic number & manual wiring
        kvChunkQueue         <- Queue.bounded[Chunk[(K, V)]](config.bufferSize)
        kvStream             <- UIO(ZStream.fromChunkQueueWithShutdown(kvChunkQueue))
        _                    <- runLoop(schemaRegistryClient, kvChunkQueue, kvStream, log).runDrain
      } yield ()

      logic.refineOrDie(tamerErrors)
    }
  }

  object Live {

    private[tamer] final def getManaged[K, V, S](
        config: KafkaConfig,
        serdes: Setup.Serdes[K, V, S],
        initialState: S,
        stateKey: Int,
        iterationFunction: (S, Queue[Chunk[(K, V)]]) => Task[S],
        repr: String
    ): ZManaged[Clock with Blocking, TamerError, Live[K, V, S]] = {

      val KafkaConfig(brokers, _, closeTimeout, _, _, StateConfig(_, groupId, clientId), properties) = config

      val consumerSettings = ConsumerSettings(brokers)
        .withProperties(properties)
        .withGroupId(groupId)
        .withClientId(clientId)
        .withCloseTimeout(closeTimeout)
        .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
      val producerSettings = ProducerSettings(brokers)
        .withProperties(properties)
        .withCloseTimeout(closeTimeout)

      val stateConsumer = Consumer.make(consumerSettings).mapError(TamerError("Could not make state consumer", _))
      val stateProducer = Producer
        .make(producerSettings, stateKeySerde.serializer, serdes.stateSerde)
        .mapError(TamerError("Could not make state producer", _))
      val valueProducer = Producer
        .make(producerSettings, serdes.keySerializer, serdes.valueSerializer)
        .mapError(TamerError("Could not make value producer", _))

      ZManaged.mapN(stateConsumer, stateProducer, valueProducer) {
        new Live(config, serdes, initialState, stateKey, iterationFunction, repr, _, _, _)
      }
    }

    private[tamer] final def getLayer[R, K, V, S](
        setup: Setup[R, K, V, S]
    ): ZLayer[R with Clock with Blocking with Has[KafkaConfig], TamerError, Has[Tamer]] =
      ZLayer.fromServiceManaged[KafkaConfig, R with Clock with Blocking, TamerError, Tamer] { config =>
        val iterationFunctionManaged = ZIO.environment[R].map(r => Function.untupled((setup.iteration _).tupled.andThen(_.provide(r)))).toManaged_
        iterationFunctionManaged.flatMap(Live.getManaged(config, setup.serdes, setup.initialState, setup.stateKey, _, setup.repr))
      }
  }

  final def live[R, K, V, S](setup: Setup[R, K, V, S]): ZLayer[R with Has[KafkaConfig] with Clock with Blocking, TamerError, Has[Tamer]] =
    Live.getLayer(setup)
}
