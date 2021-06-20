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
  def runLoop: ZIO[Clock, TamerError, Unit]
}

object Tamer {
  private[this] final case class StateKey(stateKey: String, groupId: String)
  private[this] final val stateKeySerde = Serde.key[StateKey]

  private[this] object Lag {
    final def unapply(pair: (Map[TopicPartition, Long], Map[TopicPartition, Long])): Some[Map[TopicPartition, Long]] =
      Some((pair._1.keySet ++ pair._2.keySet).foldLeft(Map.empty[TopicPartition, Long]) {
        case (acc, tp) if pair._1.contains(tp) => acc + (tp -> (pair._1(tp) - pair._2.getOrElse(tp, 0L)))
        case (acc, _)                          => acc
      })
  }

  private[this] final val tenTimes = Schedule.recurs(10) && Schedule.exponential(100.milliseconds) //FIXME make configurable

  private[this] final val tamerErrors: PartialFunction[Throwable, TamerError] = {
    case ke: KafkaException => TamerError(ke.getLocalizedMessage, ke)
    case te: TamerError     => te
  }

  private[this] implicit final class OffsetOps(private val _underlying: Offset) extends AnyVal {
    def info: String = s"${_underlying.topicPartition}@${_underlying.offset}"
  }

  private[this] final def mkRegistry(schemaRegistryClient: SchemaRegistryClient, topic: String): ULayer[RegistryInfo] =
    ZLayer.succeed(schemaRegistryClient) >>> Registry.live ++ ZLayer.succeed(topic)

  private[tamer] final def sink[K, V](
      kvStream: UStream[(K, V)],
      producer: Producer.Service[RegistryInfo, K, V],
      topic: String,
      registryLayer: ULayer[RegistryInfo],
      log: LogWriter[Task]
  ) =
    kvStream
      .map { case (k, v) => new ProducerRecord(topic, k, v) }
      .mapChunksM {
        case chunk if chunk.nonEmpty =>
          producer
            .produceChunk(chunk)
            .provideLayer(registryLayer)
            .tapError(_ => log.debug(s"failed pushing ${chunk.size} messages to $topic"))
            .retry(tenTimes) <* log.info(s"pushed ${chunk.size} messages to $topic")
        case emptyChunk => UIO(emptyChunk)
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

    // There are at least 4 possible decisions:
    //
    // 1. logEndOffsets.forAll(_ == 0L)               => initialize
    // 2. lag(logEndOffsets, currentOffset).sum == 1L => resume
    // 3. lag(logEndOffsets, currentOffset).sum == 0L => retry
    // 4. _                                           => fail
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
      val partitionOffsets = stateConsumer.endOffsets(partitionSet)
      val committedPartitionOffsets = stateConsumer.committed(partitionSet).map {
        _.map {
          case (tp, Some(o)) => Some((tp, o.offset()))
          case _             => None
        }.flatten.toMap
      }
      partitionOffsets.zip(committedPartitionOffsets).map {
        case (po, _) if po.values.forall(_ == 0L) => Initialize
        case Lag(lags) if lags.values.sum == 1L   => Resume
        case Lag(lags) if lags.values.sum == 0L   => Retry
        case _                                    => Fail
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
            .produce(stateTopic, key, initialState)
            .provideLayer(registryLayer)
            .tap(rmd => log.info(s"pushed initial state $initialState to $rmd"))
            .unit
      case Resume => log.info(s"consumer group $stateGroupId resuming consumption from $stateTopic")
      case Retry | Fail => // TODO: fix handling of retry case
        log.error(s"consumer group $stateGroupId is already at the end of $stateTopic, manual intervention required") *>
          ZIO.fail(TamerError(s"Consumer group $stateGroupId stuck at end of stream"))
    }

    val stateStream = stateConsumer
      .plainStream(stateKeySerde.deserializer, stateSerde)
      .provideLayer(registryLayer)
      .mapM {
        case CommittableRecord(record, offset) if record.key == key =>
          log.debug(s"consumer group $stateGroupId consumed state ${record.value} from ${offset.info}") *>
            // we got a valid state to process, invoke the handler
            log.debug("invoking the iteration function") *> iterationFunction(record.value, kvChunkQueue).flatMap { newState =>
              // now that te handler has concluded its job, we've got to commit the offset and push the new state to Kafka
              // we should do these two operations within a transactional boundary as there is no guarantee whatsoever that
              // both will successfully complete
              // here we're chosing to commit first before pushing the new state so we should never lag more than 1 but
              // we may repush the last batch when we resume (at least once semantics)
              offset.commitOrRetry(tenTimes) <*
                log.debug(s"consumer group $stateGroupId committed offset ${offset.info}") <*
                stateProducer
                  .produce(stateTopic, key, newState)
                  .provideLayer(registryLayer)
                  .tap(rmd => log.debug(s"pushed state $newState to $rmd"))
            }
        case CommittableRecord(record, offset) =>
          log.debug(s"consumer group $stateGroupId ignored state (wrong key: ${record.key} != $key) from ${offset.info}") *>
            offset.commitOrRetry(tenTimes) <*
            log.debug(s"consumer group $stateGroupId committed offset ${offset.info}")
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

    private[tamer] final def sinkStream(kvStream: UStream[(K, V)], registryLayer: ULayer[RegistryInfo], log: LogWriter[Task]) =
      ZStream.fromEffect(sink(kvStream, valueProducer, sinkTopic, registryLayer, log).fork <* log.info("running sink perpetually"))

    private[tamer] final def sourceStream(kvChunkQueue: Queue[Chunk[(K, V)]], registryLayer: ULayer[RegistryInfo], log: LogWriter[Task]) =
      source(
        stateTopic,
        stateGroupId,
        stateHash,
        serdes.stateSerde,
        initialState,
        stateConsumer,
        stateProducer,
        kvChunkQueue,
        registryLayer,
        iterationFunction,
        log
      )

    private[this] final def stopSource(log: LogWriter[Task]) =
      log.info(s"stopping consuming of topic $stateTopic").ignore *>
        stateConsumer.stopConsumption <*
        log.info(s"consumer of topic $stateTopic stopped").ignore

    private[tamer] final def drainSink(
        fiber: Fiber[Throwable, Unit],
        kvStream: UStream[(K, V)],
        registryLayer: ULayer[RegistryInfo],
        log: LogWriter[Task]
    ) = log.info(s"stopping producing to $sinkTopic").ignore *>
      fiber.interrupt *>
      log.info(s"producer to topic $sinkTopic stopped, running final drain on sink queue").ignore *>
      sink(kvStream, valueProducer, sinkTopic, registryLayer, log).run <*
      log.info("sink queue drained").ignore

    private[tamer] final def runLoop(
        kvChunkQueue: Queue[Chunk[(K, V)]],
        kvStream: UStream[(K, V)],
        stateRegistry: ULayer[RegistryInfo],
        sinkRegistry: ULayer[RegistryInfo],
        log: LogWriter[Task]
    ) = for {
      fiber <- sinkStream(kvStream, sinkRegistry, log)
      _     <- sourceStream(kvChunkQueue, stateRegistry, log).ensuringFirst(stopSource(log)).ensuring(drainSink(fiber, kvStream, sinkRegistry, log))
    } yield ()

    override final val runLoop: ZIO[Clock, TamerError, Unit] = {
      val logic = for {
        log                  <- logTask
        _                    <- log.info(s"initializing Tamer with setup: \n$repr")
        schemaRegistryClient <- Task(new CachedSchemaRegistryClient(config.schemaRegistryUrl, 1000)) // FIXME magic number & manual wiring
        kvChunkQueue         <- Queue.bounded[Chunk[(K, V)]](config.bufferSize)
        kvStream             <- UIO(ZStream.fromChunkQueueWithShutdown(kvChunkQueue))
        stateRegistry        <- UIO(mkRegistry(schemaRegistryClient, stateTopic))
        sinkRegistry         <- UIO(mkRegistry(schemaRegistryClient, sinkTopic))
        _                    <- runLoop(kvChunkQueue, kvStream, stateRegistry, sinkRegistry, log).runDrain
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
    ): ZManaged[Blocking with Clock, TamerError, Live[K, V, S]] = {

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
    ): ZLayer[R with Blocking with Clock with Has[KafkaConfig], TamerError, Has[Tamer]] =
      ZLayer.fromServiceManaged[KafkaConfig, R with Blocking with Clock, TamerError, Tamer] { config =>
        val iterationFunctionManaged = ZIO.environment[R].map(r => Function.untupled((setup.iteration _).tupled.andThen(_.provide(r)))).toManaged_
        iterationFunctionManaged.flatMap(Live.getManaged(config, setup.serdes, setup.initialState, setup.stateKey, _, setup.repr))
      }
  }

  final def live[R, K, V, S](setup: Setup[R, K, V, S]): ZLayer[R with Blocking with Clock with Has[KafkaConfig], TamerError, Has[Tamer]] =
    Live.getLayer(setup)
}
