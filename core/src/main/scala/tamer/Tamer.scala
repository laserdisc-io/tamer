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

  private[tamer] object Lag {
    final def unapply(pair: (Map[TopicPartition, Long], Map[TopicPartition, Long])): Some[Map[TopicPartition, Long]] =
      Some((pair._1.keySet ++ pair._2.keySet).foldLeft(Map.empty[TopicPartition, Long]) {
        case (acc, tp) if pair._1.contains(tp) => acc + (tp -> (pair._1(tp) - pair._2.getOrElse(tp, 0L)))
        case (acc, _)                          => acc
      })
    final def possiblyMigrating(map: Map[TopicPartition, Long]): Boolean =
      (map.size == 1 && map.head._2 == 1) || // old mono-partitioned topic, well behaving
        (map.size == 1 && map.head._2 == 2)  // this can happen *during* a migration *OR* a first failed transaction
    final def canResume(map: Map[TopicPartition, Long]): Boolean =
      map.values.forall(lag => lag == 1L || lag == 3L) && map.values.count(_ == 3L) == 1
  }

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

  // There are at least 4 possible decisions:
  //
  // 1. logEndOffsets.forAll(_ == 0L)               => initialize
  // 2. lags.values.forall(l => l == 1L || l == 3L) => resume
  // 3. lags.values.forall(l => l == 2L)            => migrating (from non-transactional tamer)
  // 4. _                                           => fail
  //
  // Notes:
  // the only valid lag values are 1 and 3. (and 2 for migration)
  // 1 means the commit is between a message
  // and a transaction marker for a partition that cannot be used to resume work, this
  // can happen when the state topic adds new partitions.
  //                   lag=1
  //                   ┌┴┐
  //┌ ─┌ ─┌───┬────┬─┬─┴┐│
  //   │  │n-1│n-1'│n│n'│
  //└ ─└ ─└───┴────┴─┴─▲┘
  //               committed
  //                offset
  // 1 can also mean migration when the old topic was behaving correctly
  // 3 means the commit is between the previous message and its transaction marker.
  // This partition contains the next state.
  //               lag=3
  //             ┌───┴───┐
  //┌ ─┌ ─┌───┬──┴─┬─┬──┐│
  //   │  │n-1│n-1'│n│n'│
  //└ ─└ ─└───┴──▲─┴─┴──┘▲
  //         committed  end
  //          offset   offset
  // 2 is not depicted here but will happen upon migration from the previous version of tamer
  // if it's restarted after the very first produced state (because of the missing transaction
  // marker) it should then increase to 3 upon production of the next state, it might also happen
  // upon startup when the previous run aborted the initialization transaction.
  private[tamer] sealed trait Decision                                       extends Product with Serializable
  private[tamer] final case object Initialize                                extends Decision
  private[tamer] final case object Resume                                    extends Decision
  private[tamer] final case class Migrating(lags: Map[TopicPartition, Long]) extends Decision
  private[tamer] final case class Die(lags: Map[TopicPartition, Long])       extends Decision

  private[tamer] def decidedAction(
      endOffset: Map[TopicPartition, Long],
      committedOffset: Map[TopicPartition, Long]
  ): Decision =
    (endOffset, committedOffset) match {
      case (blankPartitionMap, _) if blankPartitionMap.values.forall(_ == 0L) => Initialize
      case Lag(lags) if Lag.canResume(lags)                                   => Resume
      case Lag(lags) if Lag.possiblyMigrating(lags)                           => Migrating(lags)
      case Lag(lags)                                                          => Die(lags)
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

    val decide: Set[TopicPartition] => Task[Decision] =
      (partitionSet: Set[TopicPartition]) => {
        val partitionOffsets = consumer.endOffsets(partitionSet)
        val committedPartitionOffsets = consumer.committed(partitionSet).map {
          _.map {
            case (tp, Some(o)) => Some((tp, o.offset()))
            case _             => None
          }.flatten.toMap
        }
        partitionOffsets.zip(committedPartitionOffsets).map { case (end, committed) =>
          decidedAction(end, committed)
        }
      }

    val subscribeAndDecide = log.debug(s"subscribing to $subscription") *>
      consumer.subscribe(subscription) *>
      log.debug("awaiting assignment") *>
      waitForAssignment.flatMap(a => log.debug(s"received assignment $a") *> decide(a).tap(d => log.debug(s"decided to $d")))

    def initialize: RIO[Clock with Has[Registry], Unit] = subscribeAndDecide.flatMap {
      case Initialize =>
        log.info(s"consumer group $stateGroupId never consumed from $stateTopic") *>
          producer.createTransaction.use { t =>
            t.produce(stateTopic, key, initialState, stateKeySerde, stateValueSerde, None)
              .tap(rmd => log.info(s"pushed initial state $initialState to $rmd"))
              .unit
          }
      case Resume => log.info(s"consumer group $stateGroupId resuming consumption from $stateTopic")
      case Migrating(lags) =>
        log.warn(s"consumer group $stateGroupId resuming consumption from possibly legacy topic $stateTopic") *>
          ZIO.when(lags.values.headOption.contains(2))(
            log.warn(
              s"Lag=2 detected in a partition, if $stateGroupId stalls this might mean " +
                "that the initialization transaction failed, consider deleting the topic and trying again."
            )
          )
      case Die(lags) =>
        log.error(s"consumer group $stateGroupId had unexpected lag for one of the $stateTopic partitions, manual intervention required") *>
          ZIO.fail(TamerError(s"Consumer group $stateGroupId stuck at end of stream"))
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
        sinkFiber: Fiber[Throwable, Unit],
        queue: Dequeue[(TransactionInfo, Chunk[(K, V)])],
        log: LogWriter[Task]
    ): URIO[Has[Registry] with Clock, Unit] =
      for {
        _ <- log.info(s"stopping producing to $sinkTopic").ignore
        _ <- sinkFiber.interrupt
        _ <- log.info(s"producer to topic $sinkTopic stopped, running final drain on sink queue").ignore
        _ <- sinkStream(queue, sinkTopic, keySerializer, valueSerializer, log).runDrain.orDie.fork
        _ <- log.info("sink queue drained").ignore
        _ <- queue.size.repeatWhile(_ > 0)
        _ <- queue.shutdown
      } yield ()

    private[tamer] def runLoop(queue: Queue[(TransactionInfo, Chunk[(K, V)])], log: LogWriter[Task]) = for {
      fiber <- sinkStream(queue, sinkTopic, keySerializer, valueSerializer, log).runDrain.fork <* log.info("running sink perpetually")
      _     <- sourceStream(queue, log).ensuringFirst(stopSource(log)).ensuring(drainSink(fiber, queue, log)).runDrain
    } yield ()

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
