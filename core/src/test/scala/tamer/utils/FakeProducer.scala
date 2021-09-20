package tamer.utils

import log.effect.LogWriter
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import zio.kafka.consumer.Offset
import zio.kafka.producer.{Producer, Transaction, TransactionalProducer}
import zio.kafka.serde.Serializer
import zio.{Chunk, IO, Queue, RIO, Ref, Task, UIO, ZIO, ZManaged}

class FakeTransactionalProducer[SK, SV](val produced: Queue[ProducerRecord[SK, SV]], log: LogWriter[Task]) extends TransactionalProducer {
  override def createTransaction: ZManaged[Any, Throwable, Transaction] = ???

  class FakeTransaction extends Transaction {
    override def produce[R, K, V](topic: String, key: K, value: V, keySerializer: Serializer[R, K], valueSerializer: Serializer[R, V], offset: Option[Offset]): RIO[R, RecordMetadata] = ???
    override def produce[R, K, V](producerRecord: ProducerRecord[K, V], keySerializer: Serializer[R, K], valueSerializer: Serializer[R, V], offset: Option[Offset]): RIO[R, RecordMetadata] = ???
    override def produceChunk[R, K, V](records: Chunk[ProducerRecord[K, V]], keySerializer: Serializer[R, K], valueSerializer: Serializer[R, V], offset: Option[Offset]): RIO[R, Chunk[RecordMetadata]] = {

    }
    override def abort: IO[TransactionalProducer.UserInitiatedAbort.type, Nothing] = ???
  }
}

object FakeProducer {
  def mk[K, V](logWriter: LogWriter[Task]): UIO[FakeProducer[K, V]] =
    Queue.unbounded[ProducerRecord[K, V]].map(new FakeProducer(_, logWriter))

  def mk[K, V](producerRecordVectorRef: Queue[ProducerRecord[K, V]], logWriter: LogWriter[Task]): UIO[FakeProducer[K, V]] =
    UIO(new FakeProducer[K, V](producerRecordVectorRef, logWriter))
}
sealed class FailingFakeProducer[SK, SV](override val produced: Queue[ProducerRecord[SK, SV]], counter: Ref[Int], logWriter: LogWriter[Task])
    extends FakeProducer[SK, SV](produced, logWriter) {
  override def produceChunkAsync[R, K, V](
      records: Chunk[ProducerRecord[K, V]],
      keySerializer: Serializer[R, K],
      valueSerializer: Serializer[R, V]
  ): RIO[R, Task[Chunk[RecordMetadata]]] =
    counter.updateAndGet(_ + 1).flatMap {
      case 10 =>
        produced.offerAll(records.asInstanceOf[Chunk[ProducerRecord[SK, SV]]]) *> UIO(
          UIO(Chunk(new RecordMetadata(new TopicPartition("", 0), 0, 0, 0, 0, 0, 0)))
        )
      case n => ZIO.fail(new RuntimeException(s"expected error for testing purposes with counter $n"))
    }
}
object FailingFakeProducer {
  def mk[K, V](logWriter: LogWriter[Task]): UIO[FakeProducer[K, V]] = UIO.mapN(Queue.unbounded[ProducerRecord[K, V]], Ref.make(0)) {
    new FailingFakeProducer(_, _, logWriter)
  }
}
