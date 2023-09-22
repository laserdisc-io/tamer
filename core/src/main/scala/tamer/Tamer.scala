package tamer

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{KafkaException, TopicPartition}
import zio._
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer._
import zio.kafka.producer.{ProducerSettings, Transaction, TransactionalProducer, TransactionalProducerSettings}
import zio.kafka.serde.{Serde => ZSerde, Serializer}
import zio.stream.{Stream, ZStream}

trait Tamer {
  def runLoop: IO[TamerError, Unit]
}

object Tamer {
  case class TransactionDelimiter(promise: Promise[Nothing, Unit])
  type TransactionInfo = Either[Transaction, TransactionDelimiter]

  final case class StateKey(stateKey: String, groupId: String)

  private[this] final val tenTimes = Schedule.recurs(10L) && Schedule.exponential(100.milliseconds) // FIXME make configurable

  private[this] final val tamerErrors: PartialFunction[Throwable, TamerError] = {
    case ke: KafkaException => TamerError(ke.getLocalizedMessage, ke)
    case te: TamerError     => te
  }

  private[this] implicit final class OffsetOps(private val _underlying: Offset) extends AnyVal {
    def info: String = s"${_underlying.topicPartition}@${_underlying.offset}"
  }

  private[tamer] final def sinkStream[K, V](
      queue: Dequeue[(TransactionInfo, Chunk[(K, V)])],
      topic: String,
      keySerializer: Serializer[Any, K],
      valueSerializer: Serializer[Any, V],
      log: LogWriter[Task]
  ): Stream[Throwable,Unit] =
    ZStream
      .fromQueueWithShutdown(queue)
      .mapZIO {
        case (Left(transaction), chunk) if chunk.nonEmpty =>
          (transaction
            .produceChunk(chunk.map { case (k, v) => new ProducerRecord(topic, k, v) }, keySerializer, valueSerializer, None)
            .tapError(_ => log.debug(s"failed pushing ${chunk.size} messages to $topic"))
            .retry(tenTimes) <* log.info(s"pushed ${chunk.size} messages to $topic")).unit // TODO: stop trying if the error is transaction related
        case (Right(TransactionDelimiter(promise)), _) => promise.succeed(()).unit <* log.debug(s"user implicitly signalled end of data production")
        case _                                         => ZIO.unit <* log.debug(s"received an empty chunk for $topic")
      }

  private[tamer] sealed trait Decision        extends Product with Serializable
  private[tamer] final case object Initialize extends Decision
  private[tamer] final case object Resume     extends Decision

  private[tamer] def decidedAction(committed: Map[TopicPartition, Option[OffsetAndMetadata]]): Decision =
    if (committed.values.exists(_.isDefined)) Resume else Initialize

  private[tamer] final def source[K, V, SV](
      stateTopic: String,
      stateGroupId: String,
      stateHash: Int,
      stateKeySerde: ZSerde[Any, StateKey],
      stateValueSerde: ZSerde[Any, SV],
      initialState: SV,
      consumer: Consumer,
      producer: TransactionalProducer,
      queue: Enqueue[(TransactionInfo, Chunk[(K, V)])],
      iterationFunction: (SV, Enqueue[NonEmptyChunk[(K, V)]]) => Task[SV],
      log: LogWriter[Task]
  ): Stream[Throwable,Unit] = {

    val key          = StateKey(stateHash.toHexString, stateGroupId)
    val subscription = Subscription.topics(stateTopic)

    val partitionInfo: Task[Set[TopicPartition]] = for {
      topics        <- consumer.listTopics()
      partitionInfo <- ZIO.fromOption(topics.get(stateTopic)).mapError(_ => TamerError(s"Group $stateGroupId has no permission on topic $stateTopic"))
    } yield partitionInfo.map(pi => new TopicPartition(pi.topic(), pi.partition())).toSet

    val stateTopicDecision: Task[Decision] = for {
      _            <- log.debug(s"obtaining information on $stateTopic")
      partitionSet <- partitionInfo
      _            <- log.debug(s"received the following information on $stateTopic: $partitionSet")
      decision     <- consumer.committed(partitionSet).map(decidedAction)
      _            <- log.debug(s"decided to $decision")
    } yield decision

    val initialize: Task[Unit] = stateTopicDecision.flatMap {
      case Initialize =>
        log.info(s"consumer group $stateGroupId never consumed from $stateTopic") *>
          ZIO.scoped {
            producer.createTransaction.flatMap { tx =>
              tx.produce(stateTopic, key, initialState, stateKeySerde, stateValueSerde, None)
                .tap(rmd => log.info(s"pushed initial state $initialState to $rmd"))
                .unit
            }
          }
      case Resume => log.info(s"consumer group $stateGroupId resuming consumption from $stateTopic")
    }

    val stateStream: Stream[Throwable, Unit] = consumer
      .plainStream(subscription, stateKeySerde, stateValueSerde)
      .mapZIO {
        case cr if cr.record.key == key =>
          ZIO.scoped {
            val offset = cr.offset
            producer.createTransaction.flatMap { tx =>
              val enrichedQueue = new EnrichedBoundedEnqueue(queue, (neChunk: NonEmptyChunk[(K, V)]) => (Left(tx), neChunk.toChunk))
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
                      queue.offer((Right(TransactionDelimiter(p)), Chunk.empty)) *>
                        // whenever we get the transaction delimiter back (via promise) it means that at least production of
                        // data has been enqueue in the kafka client buffer, so we can proceed with committing the transaction
                        p.await
                    } *>
                      log.debug(s"consumer group $stateGroupId will commit offset ${offset.info}") <*
                      tx
                        .produce(stateTopic, key, newState, stateKeySerde, stateValueSerde, Some(offset))
                        .tap(rmd => log.debug(s"pushed state $newState to $rmd for $stateGroupId"))
                }
            }
          }
        case cr =>
          val offset = cr.offset
          log.debug(s"consumer group $stateGroupId ignored state (wrong key: ${cr.record.key} != $key) from ${offset.info}") *>
            offset.commitOrRetry(tenTimes) <*
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
      consumer: Consumer,
      producer: TransactionalProducer
  ) extends Tamer {

    private[this] val logTask = log4sFromName.provideEnvironment(ZEnvironment("tamer.LiveTamer"))

    private[this] val SinkConfig(sinkTopic)                    = config.sink
    private[this] val StateConfig(stateTopic, stateGroupId, _) = config.state

    private[this] val keySerializer   = serdes.keySerializer
    private[this] val valueSerializer = serdes.valueSerializer

    private[this] val stateKeySerde   = serdes.stateKeySerde
    private[this] val stateValueSerde = serdes.stateValueSerde

    private[tamer] def sourceStream(queue: Enqueue[(TransactionInfo, Chunk[(K, V)])], log: LogWriter[Task]) =
      source(
        stateTopic,
        stateGroupId,
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

    private[this] def stopSource(log: LogWriter[Task]) =
      log.info(s"stopping consuming of topic $stateTopic").ignore *>
        consumer.stopConsumption <*
        log.info(s"consumer of topic $stateTopic stopped").ignore

    private[tamer] def drainSink(
        queue: Dequeue[(TransactionInfo, Chunk[(K, V)])],
        log: LogWriter[Task]
    ): UIO[Unit] =
      for {
        _ <- log.info(s"running final drain on sink queue for topic $sinkTopic").ignore
        _ <- sinkStream(queue, sinkTopic, keySerializer, valueSerializer, log).runDrain.orDie.fork
        _ <- log.info(s"sink queue drained for $sinkTopic").ignore
        _ <- queue.size.repeatWhile(_ > 0)
        _ <- queue.shutdown
      } yield ()

    private[tamer] def runLoop(queue: Queue[(TransactionInfo, Chunk[(K, V)])], log: LogWriter[Task]) = {
      val runSink = log.info(s"running sink to $sinkTopic perpetually") *> sinkStream(queue, sinkTopic, keySerializer, valueSerializer, log).runDrain
        .onInterrupt(log.info(s"stopping producing to $sinkTopic").ignore)
      val runSource = sourceStream(queue, log).ensuring(stopSource(log)).ensuring(drainSink(queue, log)).runDrain
      runSink <&> runSource
    }

    override val runLoop: IO[TamerError, Unit] = {
      val logic = for {
        log   <- logTask
        _     <- log.info(s"initializing Tamer with setup: \n$repr")
        queue <- Queue.bounded[(TransactionInfo, Chunk[(K, V)])](config.bufferSize)
        _     <- runLoop(queue, log)
      } yield ()

      logic.refineOrDie(tamerErrors)
    }
  }

  object LiveTamer {

    private[tamer] final def getService[K, V, SV](
        config: KafkaConfig,
        mkSerdes: MkSerdes[K, V, SV],
        initialState: SV,
        stateKey: Int,
        iterationFunction: (SV, Enqueue[NonEmptyChunk[(K, V)]]) => Task[SV],
        repr: String
    ): ZIO[Scope, TamerError, LiveTamer[K, V, SV]] = {

      val KafkaConfig(brokers, _, closeTimeout, _, _, StateConfig(_, groupId, clientId), transactionalId, properties) = config

      val consumerSettings = ConsumerSettings(brokers)
        .withClientId(clientId)
        .withCloseTimeout(closeTimeout)
        .withGroupId(groupId)
        .withOffsetRetrieval(OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest))
        .withProperties(properties)
      val transactionalConsumerSettings = consumerSettings.withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
      val producerSettings = ProducerSettings(brokers)
        .withCloseTimeout(closeTimeout)
        .withProperties(properties)
      val transactionalProducerSettings = TransactionalProducerSettings(producerSettings, transactionalId)

      (Consumer.make(transactionalConsumerSettings) <*> TransactionalProducer.make(transactionalProducerSettings) <*> mkSerdes.using(config.maybeRegistry))
        .map { case (consumer, producer, serdes) =>
          new LiveTamer(config, serdes, initialState, stateKey, iterationFunction, repr, consumer, producer)
        }
        .mapError(TamerError("Could not build Kafka client", _))
    }

    private[tamer] final def getLayer[R, K: Tag, V: Tag, SV: Tag](
        setup: Setup[R, K, V, SV]
    ): ZLayer[R with KafkaConfig, TamerError, Tamer] =
      ZLayer.scoped[R with KafkaConfig] {
        for {
          config <- ZIO.service[KafkaConfig]
          res <- {
            val iterationFunction = ZIO.environment[R].map(r => Function.untupled((setup.iteration _).tupled.andThen(_.provideEnvironment(r))))
            iterationFunction.flatMap(getService(config, setup.mkSerdes, setup.initialState, setup.stateKey, _, setup.repr))
          }
        } yield res
      }
  }

  final def live[R, K: Tag, V: Tag, SV: Tag](
      setup: Setup[R, K, V, SV]
  ): ZLayer[R with KafkaConfig, TamerError, Tamer] = LiveTamer.getLayer(setup)
}
