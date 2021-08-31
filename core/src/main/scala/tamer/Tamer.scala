package tamer

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{KafkaException, TopicPartition}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.kafka.admin.AdminClient.{OffsetAndMetadata => AdminMetadata, TopicPartition => AdminPartition}
import zio.kafka.admin.{AdminClient, AdminClientSettings}
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer._
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.{Serde => ZSerde, Serializer}
import zio.stream.{UStream, ZStream}

trait Tamer {
  def runLoop: ZIO[Clock, TamerError, Unit]
}

object Tamer {
  final case class StateKey(stateKey: String, groupId: String)

  private[tamer] object Lag {
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

  private[tamer] final def sink[K, V](
      stream: UStream[(K, V)],
      producer: Producer,
      topic: String,
      keySerializer: Serializer[Has[Registry], K],
      valueSerializer: Serializer[Has[Registry], V],
      log: LogWriter[Task]
  ) =
    stream
      .map { case (k, v) => new ProducerRecord(topic, k, v) }
      .mapChunksM {
        case chunk if chunk.nonEmpty =>
          producer
            .produceChunk(chunk, keySerializer, valueSerializer)
            .tapError(_ => log.debug(s"failed pushing ${chunk.size} messages to $topic"))
            .retry(tenTimes) <* log.info(s"pushed ${chunk.size} messages to $topic")
        case emptyChunk => UIO(emptyChunk)
      }
      .runDrain

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
  private[tamer] sealed trait Decision                                            extends Product with Serializable
  private[tamer] final case object Initialize                                     extends Decision
  private[tamer] final case object Resume                                         extends Decision
  private[tamer] final case class Recover(to: Map[AdminPartition, AdminMetadata]) extends Decision
  private[tamer] final case object Die                                            extends Decision

  private[tamer] def stateOffsetCorrection(
      lags: Map[TopicPartition, Long],
      committedOffset: Map[TopicPartition, Long]
  ): Map[AdminPartition, AdminMetadata] =
    lags.foldLeft(Map.empty[AdminPartition, AdminMetadata]) { case (correction, (p, lag)) =>
      val correctOffset = committedOffset.get(p).fold(1L)(_ + lag - 1L)
      if (lag == 0 || lag > 1)
        correction.updated(
          AdminPartition(p),
          AdminMetadata(
            correctOffset,
            None,
            Some(s"Tamer offset correction. Topic: ${p.topic()}. Partition: ${p.partition()}. Lag was $lag, offset corrected to $correctOffset")
          )
        )
      else correction
    }

  private[tamer] def decidedAction(strategy: StateRecoveryStrategy)(
      endOffset: Map[TopicPartition, Long],
      committedOffset: Map[TopicPartition, Long]
  ): Decision =
    (endOffset, committedOffset) match {
      case (po, _) if po.values.forall(_ == 0L) => Initialize
      case Lag(lags) if lags.values.sum == 1L   => Resume
      case Lag(lags) =>
        strategy match {
          case ManualRecovery    => Die
          case AutomaticRecovery => Recover(stateOffsetCorrection(lags, committedOffset))
        }
    }

  private[tamer] final def source[K, V, S](
      stateTopic: String,
      stateGroupId: String,
      stateHash: Int,
      stateKeySerde: ZSerde[Has[Registry], StateKey],
      stateValueSerde: ZSerde[Has[Registry], S],
      initialState: S,
      adminClient: AdminClient,
      stateConsumer: Consumer,
      stateProducer: Producer,
      stateRecovery: StateRecoveryStrategy,
      queue: Queue[Chunk[(K, V)]],
      iterationFunction: (S, Queue[Chunk[(K, V)]]) => Task[S],
      log: LogWriter[Task]
  ) = {

    val key          = StateKey(stateHash.toHexString, stateGroupId)
    val subscription = Subscription.topics(stateTopic)

    val waitForAssignment = stateConsumer.assignment
      .withFilter(_.nonEmpty)
      .tapError(_ => log.debug(s"still no assignment on $stateGroupId, there are no partitions to process"))
      .retry(tenTimes)

    val decide: Set[TopicPartition] => Task[Decision] =
      (partitionSet: Set[TopicPartition]) => {
        val partitionOffsets = stateConsumer.endOffsets(partitionSet)
        val committedPartitionOffsets = stateConsumer.committed(partitionSet).map {
          _.map {
            case (tp, Some(o)) => Some((tp, o.offset()))
            case _             => None
          }.flatten.toMap
        }
        partitionOffsets.zip(committedPartitionOffsets).map { case (end, committed) =>
          decidedAction(stateRecovery)(end, committed)
        }
      }

    val subscribeAndDecide = log.debug(s"subscribing to $subscription") *>
      stateConsumer.subscribe(subscription) *>
      log.debug("awaiting assignment") *>
      waitForAssignment.flatMap(a => log.debug(s"received assignment $a") *> decide(a).tap(d => log.debug(s"decided to $d")))

    def initialize: RIO[Clock with Has[Registry], Unit] = subscribeAndDecide.flatMap {
      case Initialize =>
        log.info(s"consumer group $stateGroupId never consumed from $stateTopic") *>
          stateProducer
            .produce(stateTopic, key, initialState, stateKeySerde, stateValueSerde)
            .tap(rmd => log.info(s"pushed initial state $initialState to $rmd"))
            .unit
      case Resume => log.info(s"consumer group $stateGroupId resuming consumption from $stateTopic")
      case Recover(corrected) =>
        log.warn(s"incorrect consumer group's lag on $stateTopic. Group id: $stateGroupId. Unsubscribing the state consumer ...") *>
          stateConsumer.unsubscribe *>
          log.warn(s"state consumer to $stateTopic with group id $stateGroupId unsubscribed. Fixing the offset ...") *>
          log.warn(
            s"applying the following correction to $stateGroupId:\n${corrected.values.map(_.metadata.getOrElse("No Correction Metadata")).mkString("\n")}"
          ) *>
          adminClient.alterConsumerGroupOffsets(stateGroupId, corrected) *>
          log.warn(s"offset of $stateGroupId corrected. Retrying initialization of state consumer to $stateTopic with group id $stateGroupId ...") *>
          initialize
      case Die =>
        log.error(s"consumer group $stateGroupId is already at the end of $stateTopic, manual intervention required") *>
          ZIO.fail(TamerError(s"Consumer group $stateGroupId stuck at end of stream"))
    }

    val stateStream = stateConsumer
      .plainStream(stateKeySerde, stateValueSerde)
      .mapM {
        case CommittableRecord(record, offset) if record.key == key =>
          log.debug(s"consumer group $stateGroupId consumed state ${record.value} from ${offset.info}") *>
            // we got a valid state to process, invoke the handler
            log.debug("invoking the iteration function") *> iterationFunction(record.value, queue).flatMap { newState =>
              // now that the handler has concluded its job, we've got to commit the offset and push the new state to Kafka
              // we should do these two operations within a transactional boundary as there is no guarantee whatsoever that
              // both will successfully complete
              // here we're chosing to commit first before pushing the new state so we should never lag more than 1 but
              // we may repush the last batch when we resume (at least once semantics)
              offset.commitOrRetry(tenTimes) <*
                log.debug(s"consumer group $stateGroupId committed offset ${offset.info}") <*
                stateProducer
                  .produce(stateTopic, key, newState, stateKeySerde, stateValueSerde)
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
      adminClient: AdminClient,
      consumer: Consumer,
      producer: Producer
  ) extends Tamer {

    private[this] val logTask = log4sFromName.provide("tamer.kafka")

    private[this] val SinkConfig(sinkTopic)                                      = config.sink
    private[this] val StateConfig(stateTopic, stateGroupId, _, recoveryStrategy) = config.state

    private[this] val keySerializer   = serdes.keySerializer
    private[this] val valueSerializer = serdes.valueSerializer

    private[this] val stateKeySerde   = serdes.stateKeySerde
    private[this] val stateValueSerde = serdes.stateValueSerde

    private[tamer] def sinkStream(stream: UStream[(K, V)], log: LogWriter[Task]) =
      ZStream.fromEffect(sink(stream, producer, sinkTopic, keySerializer, valueSerializer, log).fork <* log.info("running sink perpetually"))

    private[tamer] def sourceStream(queue: Queue[Chunk[(K, V)]], log: LogWriter[Task]) =
      source(
        stateTopic,
        stateGroupId,
        stateHash,
        stateKeySerde,
        stateValueSerde,
        initialState,
        adminClient,
        consumer,
        producer,
        recoveryStrategy,
        queue,
        iterationFunction,
        log
      )

    private[this] def stopSource(log: LogWriter[Task]) =
      log.info(s"stopping consuming of topic $stateTopic").ignore *>
        consumer.stopConsumption <*
        log.info(s"consumer of topic $stateTopic stopped").ignore

    private[tamer] def drainSink(fiber: Fiber[Throwable, Unit], stream: UStream[(K, V)], log: LogWriter[Task]) =
      log.info(s"stopping producing to $sinkTopic").ignore *>
        fiber.interrupt *>
        log.info(s"producer to topic $sinkTopic stopped, running final drain on sink queue").ignore *>
        sink(stream, producer, sinkTopic, keySerializer, valueSerializer, log).run <*
        log.info("sink queue drained").ignore

    private[tamer] def runLoop(queue: Queue[Chunk[(K, V)]], log: LogWriter[Task]) = for {
      stream <- ZStream.succeed(ZStream.fromChunkQueueWithShutdown(queue))
      fiber  <- sinkStream(stream, log)
      _      <- sourceStream(queue, log).ensuringFirst(stopSource(log)).ensuring(drainSink(fiber, stream, log))
    } yield ()

    override val runLoop: ZIO[Clock, TamerError, Unit] = {
      val logic = for {
        log   <- logTask
        _     <- log.info(s"initializing Tamer with setup: \n$repr")
        queue <- Queue.bounded[Chunk[(K, V)]](config.bufferSize)
        _     <- runLoop(queue, log).runDrain
      } yield ()

      logic.refineOrDie(tamerErrors).provideSomeLayer[Clock](config.schemaRegistryUrl.map(Registry.live(_)).getOrElse(Registry.fake))
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

      val KafkaConfig(brokers, _, closeTimeout, _, _, StateConfig(_, groupId, clientId, _), properties) = config

      val adminClientSettings = AdminClientSettings(brokers)
        .withProperties(properties)
      val consumerSettings = ConsumerSettings(brokers)
        .withClientId(clientId)
        .withCloseTimeout(closeTimeout)
        .withGroupId(groupId)
        .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
        .withProperties(properties)
      val producerSettings = ProducerSettings(brokers)
        .withCloseTimeout(closeTimeout)
        .withProperties(properties)

      ZManaged
        .mapN(AdminClient.make(adminClientSettings), Consumer.make(consumerSettings), Producer.make(producerSettings)) {
          new Live(config, serdes, initialState, stateKey, iterationFunction, repr, _, _, _)
        }
        .mapError(TamerError("Could not build Kafka client", _))
    }

    private[tamer] final def getLayer[R, K, V, S](
        setup: Setup[R, K, V, S]
    ): ZLayer[R with Blocking with Clock with Has[KafkaConfig], TamerError, Has[Tamer]] =
      ZLayer.fromServiceManaged[KafkaConfig, R with Blocking with Clock, TamerError, Tamer] { config =>
        val iterationFunctionManaged = ZManaged.environment[R].map(r => Function.untupled((setup.iteration _).tupled.andThen(_.provide(r))))
        iterationFunctionManaged.flatMap(getManaged(config, setup.serdes, setup.initialState, setup.stateKey, _, setup.repr))
      }
  }

  final def live[R, K, V, S](
      setup: Setup[R, K, V, S]
  ): ZLayer[R with Blocking with Clock with Has[KafkaConfig], TamerError, Has[Tamer]] = Live.getLayer(setup)
}
