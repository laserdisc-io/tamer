package tamer

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{KafkaException, TopicPartition}
import zio._
import zio.kafka.admin._
import zio.kafka.admin.AdminClient.{DescribeTopicsOptions, ListTopicsOptions, NewTopic, TopicDescription}
import zio.kafka.admin.acl.AclOperation
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer._
import zio.kafka.producer.{ProducerSettings, Transaction, TransactionalProducer, TransactionalProducerSettings}
import zio.kafka.serde.{Serde => ZSerde, Serializer}
import zio.stream.{Stream, ZStream}

trait Tamer {
  def runLoop: Task[Unit]
}

object Tamer {
  private[tamer] sealed trait TxInfo extends Product with Serializable
  private[tamer] object TxInfo {
    final case class Delimiter(promise: Promise[Nothing, Unit]) extends TxInfo
    final case class Context(transaction: Transaction)          extends TxInfo
  }

  private[tamer] sealed trait StartupDecision extends Product with Serializable
  private[tamer] object StartupDecision {
    case object Initialize extends StartupDecision
    case object Resume     extends StartupDecision
  }

  final case class StateKey(stateKey: String, groupId: String)

  private final val retries = Schedule.recurs(10L) && Schedule.exponential(100.milliseconds) // FIXME make configurable

  private implicit final class OffsetOps(private val _underlying: Offset) extends AnyVal {
    def info: String = s"${_underlying.topicPartition}@${_underlying.offset}"
  }
  private implicit final class TupleOps[K, V](private val _underlying: (K, V)) extends AnyVal {
    def toProducerRecord(topic: String): ProducerRecord[K, V] = new ProducerRecord(topic, _underlying._1, _underlying._2)
  }

  private[tamer] final def sinkStream[K, V](
      sinkTopic: String,
      sinkKeySerializer: Serializer[Any, K],
      sinkValueSerializer: Serializer[Any, V],
      queue: Dequeue[(TxInfo, Chunk[(K, V)])],
      log: LogWriter[Task]
  ): Stream[Throwable, Unit] =
    ZStream
      .fromQueueWithShutdown(queue)
      .mapZIO {
        case (TxInfo.Context(transaction), chunk) if chunk.nonEmpty =>
          transaction
            .produceChunk(chunk.map(_.toProducerRecord(sinkTopic)), sinkKeySerializer, sinkValueSerializer, None)
            .tapError(_ => log.debug(s"failed pushing ${chunk.size} messages to $sinkTopic"))
            .retry(retries) // TODO: stop trying if the error is transaction related
            .unit <*
            log.info(s"pushed ${chunk.size} messages to $sinkTopic")
        case (TxInfo.Delimiter(promise), _) =>
          promise.succeed(()).unit <*
            log.debug(s"user implicitly signalled end of data production")
        case _ => log.debug(s"received an empty chunk for $sinkTopic")
      }

  private[tamer] final def sourceStream[K, V, SV](
      stateTopic: String,
      stateGroupId: String,
      stateHash: Int,
      stateKeySerde: ZSerde[Any, StateKey],
      stateValueSerde: ZSerde[Any, SV],
      initialState: SV,
      consumer: Consumer,
      producer: TransactionalProducer,
      queue: Enqueue[(TxInfo, Chunk[(K, V)])],
      iterationFunction: (SV, Enqueue[NonEmptyChunk[(K, V)]]) => Task[SV],
      log: LogWriter[Task]
  ): Stream[Throwable, Unit] = {

    val key          = StateKey(stateHash.toHexString, stateGroupId)
    val subscription = Subscription.topics(stateTopic)

    val partitionSet = consumer.partitionsFor(stateTopic).map(_.map(pi => new TopicPartition(pi.topic(), pi.partition())).toSet)

    val startupDecision: Task[StartupDecision] = for {
      _          <- log.debug(s"obtaining information on $stateTopic")
      partitions <- partitionSet
      _          <- log.debug(s"received the following information on $stateTopic: $partitionSet")
      decision   <- consumer.committed(partitions).map(c => if (c.values.exists(_.isDefined)) StartupDecision.Resume else StartupDecision.Initialize)
      _          <- log.debug(s"decided to $decision")
    } yield decision

    val initialize: Task[Unit] = startupDecision.flatMap {
      case StartupDecision.Initialize =>
        log.info(s"consumer group $stateGroupId never consumed from $stateTopic") *>
          ZIO.scoped {
            producer.createTransaction.flatMap { transaction =>
              transaction
                .produce(stateTopic, key, initialState, stateKeySerde, stateValueSerde, None)
                .tap(rmd => log.info(s"pushed initial state $initialState to $rmd"))
                .unit
            }
          }
      case StartupDecision.Resume => log.info(s"consumer group $stateGroupId resuming consumption from $stateTopic")
    }

    val stateStream: Stream[Throwable, Unit] = consumer
      .plainStream(subscription, stateKeySerde, stateValueSerde)
      .mapZIO {
        case cr if cr.record.key == key =>
          ZIO.scoped {
            val offset = cr.offset
            producer.createTransaction.flatMap { transaction =>
              val enrichedQueue =
                new EnrichedBoundedEnqueue(queue, (neChunk: NonEmptyChunk[(K, V)]) => (TxInfo.Context(transaction), neChunk.toChunk))
              log.debug(s"consumer group $stateGroupId consumed state ${cr.record.value} from ${offset.info}") *>
                // we got a valid state to process, invoke the handler
                log.debug(s"invoking the iteration function under $stateGroupId") *> iterationFunction(cr.record.value, enrichedQueue).flatMap {
                  newState =>
                    // now that the handler has concluded its job, we've got to commit the offset and push the new state to Kafka
                    // we do these two operations within a transactional boundary as there is no guarantee whatsoever that
                    // both will successfully complete
                    Promise.make[Nothing, Unit].flatMap { p =>
                      // we enqueue a transaction delimiter because at this point, since we have a newState, we presume that
                      // the user has finished publishing data to the queue
                      queue.offer((TxInfo.Delimiter(p), Chunk.empty)) *>
                        // whenever we get the transaction delimiter back (via promise) it means that at least production of
                        // data has been enqueue in the kafka client buffer, so we can proceed with committing the transaction
                        p.await
                    } *>
                      log.debug(s"consumer group $stateGroupId will commit offset ${offset.info}") <*
                      transaction
                        .produce(stateTopic, key, newState, stateKeySerde, stateValueSerde, Some(offset))
                        .tap(rmd => log.debug(s"pushed state $newState to $rmd for $stateGroupId"))
                }
            }
          }
        case cr =>
          val offset = cr.offset
          log.debug(s"consumer group $stateGroupId ignored state (wrong key: ${cr.record.key} != $key) from ${offset.info}") *>
            offset.commitOrRetry(retries) <*
            log.debug(s"consumer group $stateGroupId committed offset ${offset.info}")
      }

    ZStream.fromZIO(initialize).drain ++ stateStream
  }

  final class LiveTamer[K, V, SV](
      config: KafkaConfig,
      serdes: Serdes[K, V, SV],
      initialState: SV,
      stateHash: Int,
      iterationFunction: (SV, Enqueue[NonEmptyChunk[(K, V)]]) => Task[SV],
      repr: String,
      adminClient: AdminClient,
      consumer: Consumer,
      producer: TransactionalProducer
  ) extends Tamer {

    private val logTask = log4sFromName.provideEnvironment(ZEnvironment("tamer.LiveTamer"))

    private val sinkTopicConfig @ TopicConfig(sinkTopicName, maybeSinkTopicOptions)    = config.sink
    private val stateTopicConfig @ TopicConfig(stateTopicName, maybeStateTopicOptions) = config.state

    private val keySerializer   = serdes.keySerializer
    private val valueSerializer = serdes.valueSerializer

    private val stateKeySerde   = serdes.stateKeySerde
    private val stateValueSerde = serdes.stateValueSerde

    private def source(queue: Enqueue[(TxInfo, Chunk[(K, V)])], log: LogWriter[Task]) =
      sourceStream(
        stateTopicName,
        config.groupId,
        stateHash,
        stateKeySerde,
        stateValueSerde,
        initialState,
        consumer,
        producer,
        queue,
        iterationFunction,
        log
      )

    private def stopSource(log: LogWriter[Task]) =
      log.info(s"stopping consuming of topic $stateTopicName").ignore *>
        consumer.stopConsumption <*
        log.info(s"consumer of topic $stateTopicName stopped").ignore

    private def sink(queue: Dequeue[(TxInfo, Chunk[(K, V)])], log: LogWriter[Task]) =
      sinkStream(config.sink.topicName, keySerializer, valueSerializer, queue, log)

    private def drainSink(queue: Dequeue[(TxInfo, Chunk[(K, V)])], log: LogWriter[Task]) =
      log.info(s"running final drain on sink queue for topic $sinkTopicName").ignore *>
        sink(queue, log).runDrain.orDie.fork <*
        log.info(s"sink queue drained for $sinkTopicName").ignore *>
        queue.size.repeatWhile(_ > 0) *>
        queue.shutdown

    private def runLoop(queue: Queue[(TxInfo, Chunk[(K, V)])], log: LogWriter[Task]) = {
      val runSink = log.info(s"running sink on topic $sinkTopicName perpetually") *>
        sink(queue, log).runDrain.onInterrupt(log.info(s"stopping producing to $sinkTopicName").ignore)
      val runSource = source(queue, log).ensuring(stopSource(log)).ensuring(drainSink(queue, log)).runDrain

      runSink <&> runSource
    }

    private final val listTopicsOptions     = Some(ListTopicsOptions(listInternal = false, timeout = None))
    private final val describeTopicsOptions = Some(DescribeTopicsOptions(includeAuthorizedOperations = true, timeout = None))

    private def filterSinkAndState(topic: String): Boolean = topic == sinkTopicName || topic == stateTopicName

    private def describeTopics(log: LogWriter[Task]) = for {
      topics         <- adminClient.listTopics(listTopicsOptions).map(_.keySet.filter(filterSinkAndState))
      maybeTopicDesc <- ZIO.when(topics.nonEmpty)(log.debug(s"describing $topics") *> adminClient.describeTopics(topics, describeTopicsOptions))
      _              <- log.debug(s"result of describing $topics: $maybeTopicDesc")
    } yield maybeTopicDesc.getOrElse(Map.empty)

    private def verifyOrCreateTopic(
        topics: Map[String, TopicDescription],
        topicConfig: TopicConfig,
        expectedACL: Set[AclOperation],
        log: LogWriter[Task]
    ) =
      ZIO
        .fromOption(topics.get(topicConfig.topicName))
        .mapBoth(_ => topicConfig.maybeTopicOptions, (_, topicConfig.maybeTopicOptions))
        .foldZIO(
          {
            case Some(TopicOptions(partitions, replicas)) =>
              log.info(
                s"topic ${topicConfig.topicName} does not exist in Kafka. Given auto_create is set to true, creating it with $partitions partitions and $replicas replicas"
              ) *> adminClient.createTopic(NewTopic(topicConfig.topicName, partitions, replicas))
            case _ =>
              ZIO.fail(
                TamerError(s"Topic ${topicConfig.topicName} does not exist in Kafka and its corresponding auto_create flag is set to false, aborting")
              )
          },
          {
            case (TopicDescription(_, _, tpInfo, Some(acls)), Some(TopicOptions(partitions, replicas)))
                if tpInfo.size == partitions && tpInfo.forall(_.replicas.size == replicas) && expectedACL.subsetOf(acls) =>
              log
                .info(
                  s"verified topic ${topicConfig.topicName} successfully. It has the expected $partitions partitions and expected $replicas replicas, and it satisfies all expected ACLs (${expectedACL
                      .mkString(", ")}), proceeding"
                )
            case (TopicDescription(_, _, tpInfo, Some(acls)), None) if expectedACL.subsetOf(acls) =>
              log
                .info(
                  s"verified topic ${topicConfig.topicName} successfully. Kafka informs us that it has ${tpInfo.size} partitions and ${tpInfo.head.replicas.size} replicas, but it satisfies all expected ACLs (${expectedACL
                      .mkString(", ")}), proceeding"
                )
            case (TopicDescription(_, _, tpInfo, Some(acls)), Some(TopicOptions(partitions, replicas))) if expectedACL.subsetOf(acls) =>
              log
                .warn(
                  s"inconsistencies in topic ${topicConfig.topicName}. Kafka informs us that it has ${tpInfo.size} partitions and ${tpInfo.head.replicas.size} replicas, expecting $partitions partitions and ${tpInfo.head.replicas.size} replicas, but it satisfies all expected ACLs (${expectedACL
                      .mkString(", ")}), proceeding"
                )
            case (TopicDescription(_, _, _, Some(acls)), _) =>
              ZIO.fail(
                TamerError(
                  s"Topic ${topicConfig.topicName} does not satisfy all expected ACLs. Kafka informs us that it has ${acls
                      .mkString(", ")}, expecting ${expectedACL.mkString(", ")}"
                )
              )
            case (other, _) =>
              ZIO.fail(
                TamerError(s"Topic ${topicConfig.topicName} cannot be verified. Kafka informs us the following about this topic: $other")
              )
          }
        )

    private def initTopics(log: LogWriter[Task]) = for {
      topics <- describeTopics(log)
      _      <- verifyOrCreateTopic(topics, sinkTopicConfig, Set(AclOperation.Write), log)
      _      <- verifyOrCreateTopic(topics, stateTopicConfig, Set(AclOperation.Read, AclOperation.Write), log)
    } yield ()

    override val runLoop: Task[Unit] = for {
      log   <- logTask
      _     <- log.info(s"initializing Tamer with setup:\n$repr")
      _     <- initTopics(log)
      queue <- Queue.bounded[(TxInfo, Chunk[(K, V)])](config.bufferSize)
      _     <- runLoop(queue, log)
    } yield ()
  }

  object LiveTamer {

    private[tamer] final def getService[K, V, SV](
        config: KafkaConfig,
        serdesProvider: SerdesProvider[K, V, SV],
        initialState: SV,
        stateKey: Int,
        iterationFunction: (SV, Enqueue[NonEmptyChunk[(K, V)]]) => Task[SV],
        repr: String
    ): RIO[Scope, LiveTamer[K, V, SV]] = {

      val KafkaConfig(brokers, _, closeTimeout, _, _, _, groupId, clientId, transactionalId, properties) = config

      val adminClientSettings = AdminClientSettings(closeTimeout, Map.empty)
        .withBootstrapServers(brokers)
      val consumerSettings = ConsumerSettings(brokers)
        .withClientId(clientId)
        .withCloseTimeout(closeTimeout)
        .withGroupId(groupId)
        .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
        .withProperties(properties)
        .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
      val producerSettings = ProducerSettings(brokers)
        .withCloseTimeout(closeTimeout)
        .withProperties(properties)
      val txProducerSettings = TransactionalProducerSettings(producerSettings, transactionalId)

      val serdes      = serdesProvider.using(config.maybeRegistry)
      val adminClient = AdminClient.make(adminClientSettings)
      val consumer    = Consumer.make(consumerSettings)
      val producer    = TransactionalProducer.make(txProducerSettings)

      (serdes <*> adminClient <*> consumer <*> producer)
        .map { case (serdes, adminClient, consumer, producer) =>
          new LiveTamer(config, serdes, initialState, stateKey, iterationFunction, repr, adminClient, consumer, producer)
        }
        .mapError(TamerError("Could not build Kafka client", _))
    }

    private[tamer] final def getLayer[R, K: Tag, V: Tag, SV: Tag](setup: Setup[R, K, V, SV]): RLayer[R with KafkaConfig, Tamer] =
      ZLayer.scoped[R with KafkaConfig] {
        for {
          config <- ZIO.service[KafkaConfig]
          res <- {
            val iterationFunction = ZIO.environment[R].map(r => Function.untupled((setup.iteration _).tupled.andThen(_.provideEnvironment(r))))
            iterationFunction.flatMap(getService(config, setup.serdesProvider, setup.initialState, setup.stateKey, _, setup.repr))
          }
        } yield res
      }
  }

  final def live[R, K: Tag, V: Tag, SV: Tag](setup: Setup[R, K, V, SV]): RLayer[R with KafkaConfig, Tamer] = LiveTamer.getLayer(setup)
}
