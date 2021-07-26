package utils

import log.effect.LogWriter
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import zio.kafka.producer.Producer.Service
import zio.{Chunk, Queue, RIO, Ref, Task, UIO, ZIO}

sealed class FakeProducer[R, K, V](val produced: Queue[ProducerRecord[K, V]], log: LogWriter[Task]) extends Service[R, K, V] {
  override def produce(record: ProducerRecord[K, V]): RIO[R, RecordMetadata] = ???
  override def produce(topic: String, key: K, value: V): RIO[R, RecordMetadata] = produced.offer(new ProducerRecord(topic, key, value)) *>
    log.info(s"producer fakely offered record '$key:$value' to topic '$topic-0'") *>
    RIO(new RecordMetadata(new TopicPartition(topic, 0), 0, 0, 0, 0, 0, 0))
  override def produceAsync(record: ProducerRecord[K, V]): RIO[R, Task[RecordMetadata]]                     = ???
  override def produceAsync(topic: String, key: K, value: V): RIO[R, Task[RecordMetadata]]                  = ???
  override def produceChunkAsync(records: Chunk[ProducerRecord[K, V]]): RIO[R, Task[Chunk[RecordMetadata]]] = ???
  override def produceChunk(records: Chunk[ProducerRecord[K, V]]): RIO[R, Chunk[RecordMetadata]] =
    produced.offerAll(records) *> UIO(Chunk(new RecordMetadata(new TopicPartition("", 0), 0, 0, 0, 0, 0, 0)))
  override def flush: Task[Unit]                      = ???
  override def metrics: Task[Map[MetricName, Metric]] = ???
}
object FakeProducer {
  def mk[R, K, V](logWriter: LogWriter[Task]): UIO[FakeProducer[R, K, V]] =
    Queue.unbounded[ProducerRecord[K, V]].map(new FakeProducer(_, logWriter))

  def mk[R, K, V](producerRecordVectorRef: Queue[ProducerRecord[K, V]], logWriter: LogWriter[Task]): UIO[FakeProducer[R, K, V]] =
    UIO(new FakeProducer[R, K, V](producerRecordVectorRef, logWriter))
}
sealed class FailingFakeProducer[R, K, V](override val produced: Queue[ProducerRecord[K, V]], counter: Ref[Int], logWriter: LogWriter[Task])
    extends FakeProducer[R, K, V](produced, logWriter) {
  override def produceChunkAsync(records: Chunk[ProducerRecord[K, V]]): RIO[R, Task[Chunk[RecordMetadata]]] =
    counter.updateAndGet(_ + 1).flatMap {
      case 10 =>
        produced.offerAll(records) *> UIO(UIO(Chunk(new RecordMetadata(new TopicPartition("", 0), 0, 0, 0, 0, 0, 0))))
      case n => ZIO.fail(new RuntimeException(s"expected error for testing purposes with counter $n"))
    }
}
object FailingFakeProducer {
  def mk[R, K, V](logWriter: LogWriter[Task]): UIO[FakeProducer[R, K, V]] = UIO.mapN(Queue.unbounded[ProducerRecord[K, V]], Ref.make(0)) {
    new FailingFakeProducer(_, _, logWriter)
  }
}
