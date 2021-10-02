package tamer

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{KafkaException, TopicPartition}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer._
import zio.kafka.producer.{ProducerSettings, Transaction, TransactionalProducer, TransactionalProducerSettings}
import zio.kafka.serde.{Serde => ZSerde, Serializer}
import zio.stream.ZStream

trait Tamer {
  def runLoop: ZIO[Clock, TamerError, Unit]
}

object Tamer {
  case class TransactionDelimiter(promise: Promise[Nothing, Unit])
  type TransactionInfo = Either[Transaction, TransactionDelimiter]

  final case class StateKey(stateKey: String, groupId: String)

  private[this] final val tenTimes = Schedule.recurs(10) && Schedule.exponential(100.milliseconds) //FIXME make configurable

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
      keySerializer: Serializer[Has[Registry], K],
      valueSerializer: Serializer[Has[Registry], V],
      log: LogWriter[Task]
  ) =
    ZStream.fromQueueWithShutdown(
      queue
        .mapM {
          case (Left(transaction), chunk) if chunk.nonEmpty =>
            (transaction
              .produceChunk(chunk.map { case (k, v) => new ProducerRecord(topic, k, v) }, keySerializer, valueSerializer, None)
              .tapError(_ => log.debug(s"failed pushing ${chunk.size} messages to $topic"))
              .retry(tenTimes) <* log.info(s"pushed ${chunk.size} messages to $topic")).unit // TODO: stop trying if the error is transaction related
          case (Right(TransactionDelimiter(promise)), _) => promise.succeed(()) <* log.debug(s"user implicitly signaled end of data production")
          case _                                         => UIO.unit <* log.debug(s"received an empty chunk for $topic")
        }
    )

  private[tamer] sealed trait Decision        extends Product with Serializable
  private[tamer] final case object Initialize extends Decision
  private[tamer] final case object Resume     extends Decision

  private[tamer] def decidedAction(
      endOffset: Map[TopicPartition, Long]
  ): Decision =
    endOffset match {
      case blankPartitionMap if blankPartitionMap.values.forall(_ == 0L) => Initialize
      case _                                                             => Resume
    }

  private[tamer] final def source[K, V, S](
      stateTopic: String,
      stateGroupId: String,
      stateHash: Int,
      stateKeySerde: ZSerde[Has[Registry], StateKey],
      stateValueSerde: ZSerde[Has[Registry], S],
      initialState: S,
      consumer: Consumer,
      producer: TransactionalProducer,
      queue: Enqueue[(TransactionInfo, Chunk[(K, V)])],
      iterationFunction: (S, Enqueue[NonEmptyChunk[(K, V)]]) => Task[S],
      log: LogWriter[Task]
  ) = {

    val key          = StateKey(stateHash.toHexString, stateGroupId)
    val subscription = Subscription.topics(stateTopic)

    val waitForAssignment = consumer.assignment
      .withFilter(_.nonEmpty)
      .tapError(_ => log.debug(s"still no assignment on $stateGroupId, there are no partitions to process"))
      .retry(tenTimes)

    val subscribeAndDecide = log.debug(s"subscribing to $subscription") *>
      consumer.subscribe(subscription) *>
      log.debug("awaiting assignment") *>
      waitForAssignment.flatMap(partitionSet =>
        log.debug(s"received assignment $partitionSet") *>
          consumer.endOffsets(partitionSet).map(decidedAction).tap(d => log.debug(s"decided to $d"))
      )

    def initialize: RIO[Clock with Has[Registry], Unit] = subscribeAndDecide.flatMap {
      case Initialize =>
        log.info(s"consumer group $stateGroupId never consumed from $stateTopic") *>
          producer.createTransaction.use { t =>
            t.produce(stateTopic, key, initialState, stateKeySerde, stateValueSerde, None)
              .tap(rmd => log.info(s"pushed initial state $initialState to $rmd"))
              .unit
          }
      case Resume => log.info(s"consumer group $stateGroupId resuming consumption from $stateTopic")
    }

    val stateStream = consumer
      .plainStream(stateKeySerde, stateValueSerde)
      .mapM {
        case CommittableRecord(record, offset) if record.key == key =>
          producer.createTransaction.use { transaction =>
            val enrichedQueue = queue.contramap[NonEmptyChunk[(K, V)]](nonEmptyChunk => (Left(transaction), nonEmptyChunk.toChunk))
            log.debug(s"consumer group $stateGroupId consumed state ${record.value} from ${offset.info}") *>
              // we got a valid state to process, invoke the handler
              log.debug(s"invoking the iteration function under $stateGroupId") *> iterationFunction(record.value, enrichedQueue).flatMap {
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
                    log.debug(s"consumer group $stateGroupId will committ offset ${offset.info}") <*
                    transaction
                      .produce(stateTopic, key, newState, stateKeySerde, stateValueSerde, Some(offset))
                      .tap(rmd => log.debug(s"pushed state $newState to $rmd for $stateGroupId"))
              }
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
      iterationFunction: (S, Enqueue[NonEmptyChunk[(K, V)]]) => Task[S],
      repr: String,
      consumer: Consumer,
      producer: TransactionalProducer
  ) extends Tamer {

    private[this] val logTask = log4sFromName.provide("tamer.kafka")

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
    ): URIO[Has[Registry] with Clock, Unit] =
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
      val runSource = sourceStream(queue, log).ensuringFirst(stopSource(log)).ensuring(drainSink(queue, log)).runDrain
      runSink <&> runSource
    }

    override val runLoop: ZIO[Clock, TamerError, Unit] = {
      val logic = for {
        log   <- logTask
        _     <- log.info(s"initializing Tamer with setup: \n$repr")
        queue <- Queue.bounded[(TransactionInfo, Chunk[(K, V)])](config.bufferSize)
        _     <- runLoop(queue, log)
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
        iterationFunction: (S, Enqueue[NonEmptyChunk[(K, V)]]) => Task[S],
        repr: String
    ): ZManaged[Blocking with Clock, TamerError, Live[K, V, S]] = {

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

      ZManaged
        .mapN(
          Consumer.make(transactionalConsumerSettings),
          TransactionalProducer.make(transactionalProducerSettings)
        ) {
          new Live(config, serdes, initialState, stateKey, iterationFunction, repr, _, _)
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
