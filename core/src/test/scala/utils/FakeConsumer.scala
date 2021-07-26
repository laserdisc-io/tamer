package utils

import log.effect.LogWriter
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata, OffsetAndTimestamp}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}
import utils.FakeConsumerUtils.{getCommittedOr0, increaseForPartition}
import zio.clock.Clock
import zio.duration.Duration
import zio.kafka.consumer.Consumer.Service
import zio.kafka.consumer.{CommittableRecord, SubscribedConsumer, Subscription}
import zio.kafka.serde.Deserializer
import zio.random.Random
import zio.stream.ZStream
import zio.stream.ZStream.repeatEffectOption
import zio.{Cause, IO, Queue, Ref, Schedule, Task, UIO, URIO, ZIO, ZQueue, stream}

/** Topic is fixed to 'topic'
  */
sealed class FakeConsumer[IK, IV](
    val committed: Ref[Map[TopicPartition, Option[Long]]],
    val inFlight: Queue[(TopicPartition, ProducerRecord[IK, IV])],
    val produced: Map[TopicPartition, Queue[ProducerRecord[IK, IV]]],
    log: LogWriter[Task]
) extends Service {
  protected def checkThatThereIsExactly1TopicPartition(partitions: Set[TopicPartition]): IO[IllegalArgumentException, TopicPartition] = for {
    partition <- ZIO.fromOption(partitions.headOption).orElseFail(new IllegalArgumentException("You passed an empty set of TopicPartition"))
    _ <- ZIO.when(partitions.size > 1)(
      ZIO.fail(new IllegalArgumentException("There is only 1 TopicPartition in this FakeConsumer, you passed more"))
    )
  } yield partition
  override def assignment: Task[Set[TopicPartition]] = for {
    partitions <- committed.get.map(_.keySet)
    _          <- log.info(s"consumer fakely returning assignment to topic 'topic' on partitions $partitions")
  } yield partitions
  override def beginningOffsets(partitions: Set[TopicPartition], timeout: Duration): Task[Map[TopicPartition, Long]] = ???
  override def committed(partitions: Set[TopicPartition], timeout: Duration): Task[Map[TopicPartition, Option[OffsetAndMetadata]]] =
    for {
      localPartitions <- committed.get.map(_.keySet)
      _               <- ZIO.when(localPartitions != partitions)(ZIO.fail(new IllegalArgumentException("passed partitions must be the same")))
      committed       <- committed.get
      latestCommittedOffset = partitions
        .map(parameterPartition => parameterPartition -> committed(parameterPartition).map(new OffsetAndMetadata(_)))
        .toMap
      _ <- log.info(s"consumer fakely returning in memory committed offset of $committed")
    } yield latestCommittedOffset

  override def endOffsets(partitions: Set[TopicPartition], timeout: Duration): Task[Map[TopicPartition, Long]] = for {
    producedSizes  <- ZIO.foreach(produced)({ case (topicPartition, queue) => queue.size.map((topicPartition, _)) })
    initialOffsets <- committed.get
    endOffsets <- Task(initialOffsets.map { case (topicPartition, initialOffset) =>
      // depends on kafka read mode
      val producedSizesLong = producedSizes
        .getOrElse(topicPartition, throw new RuntimeException("queues topic partition set and initial offsets must match"))
        .longValue
      topicPartition -> (producedSizesLong + initialOffset.getOrElse(0L)).longValue
    })
    _ <- log.info(s"consumer fakely reporting in memory end offset of $endOffsets")
  } yield endOffsets

  override def listTopics(timeout: Duration): Task[Map[String, List[PartitionInfo]]] = ???
  override def partitionedStream[R, K, V](
      keyDeserializer: Deserializer[R, K],
      valueDeserializer: Deserializer[R, V]
  ): stream.Stream[Throwable, (TopicPartition, ZStream[R, Throwable, CommittableRecord[K, V]])] = ???
  private object Pull {
    def halt[E](c: Cause[E]): IO[Option[E], Nothing] = IO.halt(c).mapError(Some(_))
    val end: IO[Option[Nothing], Nothing]            = IO.fail(None)
  }
  private def fromTamerQueue[R, E](
      queue: ZQueue[Nothing, R, Any, E, Nothing, (TopicPartition, ProducerRecord[IK, IV])],
      inFlight: Queue[(TopicPartition, ProducerRecord[IK, IV])]
  ): ZStream[R, E, (TopicPartition, ProducerRecord[IK, IV])] =
    repeatEffectOption {
      queue.take
        .tap(value => inFlight.offer(value))
        .catchAllCause(c =>
          queue.isShutdown.flatMap { down =>
            if (down && c.interrupted) Pull.end
            else Pull.halt(c)
          }
        )
    }

  override def plainStream[R, K, V](
      keyDeserializer: Deserializer[R, K],
      valueDeserializer: Deserializer[R, V],
      outputBuffer: Int
  ): ZStream[R, Throwable, CommittableRecord[K, V]] =
    for {
      initialOffsets <- getCommittedOr0(committed)
      counters       <- ZStream.fromEffect(Ref.make(initialOffsets))
      partitionQueues = produced.map { case (partition, queue) => queue.map(record => (partition, record)) }
      stream <- partitionQueues
        .map(partitionQueue =>
          fromTamerQueue(partitionQueue, inFlight)
            .tap { case (partition, value) => log.info(s"consumer fakely taking ($partition, ${value.value()}) from a queue") }
        )
        .fold(ZStream.empty) { case (a, b) => a.interleaveWith(b)(ZStream.repeatEffect(zio.random.nextBoolean.provideLayer(Random.live))) }
        .mapM { case (partition, record) =>
          for {
            offset <- counters
              .getAndUpdate(m => increaseForPartition(partition, m))
              .map(_.get(partition))
              .map(_.toRight(new RuntimeException(s"Counters did not contain partition $partition")))
              .absolve
          } yield CommittableRecord(
            record = new ConsumerRecord("topic", 0, offset, record.key().asInstanceOf[K], record.value().asInstanceOf[V]),
            commitHandle = (toCommit: Map[TopicPartition, Long]) =>
              (for {
                r <- Task(new java.util.Random().nextInt(1)).map(_.longValue) // simulates the asynchronicity
                _ <- Task(Thread.sleep(r)) // simulates the asynchronicity
                _ <- {
                  val toCommitOffset = toCommit(partition)
                  log.info(s"consumer fakely committing offset $toCommitOffset") *>
                    inFlight.take // TODO: make sure we take the expected value
                      .tap { case (partition, value) => log.info(s"consumer fakely removing ($partition -> ${value.value()}) from in-flight") } *>
                    committed.update(_.updated(partition, Some(toCommitOffset)))
                }
              } yield ()).fork.unit // simulates the asynchronicity (remove this and the above when using kafka transactions)
          )
        }
        .tap(committableRecord => log.info(s"consumer fakely emitting an element summarized as: ${committableRecord.key}:${committableRecord.value}"))
    } yield stream

  override def stopConsumption: UIO[Unit] = ???
  override def consumeWith[R, RC, K, V](
      subscription: Subscription,
      keyDeserializer: Deserializer[R, K],
      valueDeserializer: Deserializer[R, V],
      commitRetryPolicy: Schedule[Clock, Any, Any]
  )(f: (K, V) => URIO[RC, Unit]): ZIO[R with RC with Clock, Throwable, Unit] = ???
  override def subscribe(subscription: Subscription): Task[Unit] =
    log.info(s"consumer fakely subscribing to $subscription").unit
  override def unsubscribe: Task[Unit]                                                                                                  = ???
  override def offsetsForTimes(timestamps: Map[TopicPartition, Long], timeout: Duration): Task[Map[TopicPartition, OffsetAndTimestamp]] = ???
  override def partitionsFor(topic: String, timeout: Duration): Task[List[PartitionInfo]]                                               = ???
  override def position(partition: TopicPartition, timeout: Duration): Task[Long]                                                       = ???
  override def subscribeAnd(subscription: Subscription): SubscribedConsumer                                                             = ???
  override def subscription: Task[Set[String]]                                                                                          = ???
  override def metrics: Task[Map[MetricName, Metric]]                                                                                   = ???
}

object FakeConsumer {
  val partition0 = new TopicPartition("topic", 0)

  def mk[K, V](producerRecordQueue: Queue[ProducerRecord[K, V]], logWriter: LogWriter[Task]): UIO[FakeConsumer[K, V]] = for {
    committed <- Ref.make(Map(partition0 -> Option(0L)))
    inFlight  <- Queue.unbounded[(TopicPartition, ProducerRecord[K, V])]
  } yield new FakeConsumer(committed, inFlight, Map(partition0 -> producerRecordQueue), logWriter)

  def mk[K, V](
      producerRecordQueue: Queue[ProducerRecord[K, V]],
      inFlightQueue: Queue[(TopicPartition, ProducerRecord[K, V])],
      logWriter: LogWriter[Task]
  ): UIO[FakeConsumer[K, V]] = for {
    committed <- Ref.make(Map(partition0 -> Option(0L)))
  } yield new FakeConsumer(committed, inFlightQueue, Map(partition0 -> producerRecordQueue), logWriter)

  def mk[K, V](initialCommitted: Int, producerRecordQueue: Queue[ProducerRecord[K, V]], logWriter: LogWriter[Task]): UIO[FakeConsumer[K, V]] = for {
    committed <- Ref.make(Map(partition0 -> Option(initialCommitted.longValue)))
    inFlight  <- Queue.unbounded[(TopicPartition, ProducerRecord[K, V])]
  } yield new FakeConsumer(committed, inFlight, Map(partition0 -> producerRecordQueue), logWriter)
}
