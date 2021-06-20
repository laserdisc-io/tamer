package tamer

import io.confluent.kafka.schemaregistry.ParsedSchema
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import zio._
import zio.blocking.Blocking
import zio.duration.durationInt
import zio.kafka.producer.Producer.Service
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.environment.TestClock
import zio.test.{DefaultRunnableSpec, assert}

object SinkSpec extends DefaultRunnableSpec {
  private[this] def nullRegistryInfoFor(topic: String) = ZLayer.succeed(topic) ++ ZLayer.succeed(new Registry {
    def getOrRegisterId(subject: String, schema: ParsedSchema): Task[Int] = ???
    def verifySchema(id: Int, schema: ParsedSchema): Task[Unit]           = ???
  })

  override final val spec = suite("SinkSpec")(
    testM("should correctly produce") {
      for {
        log      <- log4sFromName.provide("test1")
        producer <- FakeProducer.mk[RegistryInfo, Key, Value]
        _        <- Tamer.sink(ZStream.repeat(Key(42) -> Value(42)).take(1), producer, "topic", nullRegistryInfoFor("topic"), log)
        records  <- producer.produced.get
      } yield assert(records)(equalTo(Vector(new ProducerRecord("topic", Key(42), Value(42)))))
    },
    testM("should correctly produce in case of moderate jitter") {
      for {
        log      <- log4sFromName.provide("test2")
        producer <- FailingFakeProducer.mk[RegistryInfo, Key, Value]
        fiber    <- Tamer.sink(ZStream.repeat(Key(42) -> Value(42)).take(1), producer, "topic", nullRegistryInfoFor("topic"), log).fork
        _        <- TestClock.adjust(5.second).repeat(Schedule.recurs(10))
        _        <- fiber.join
        records  <- producer.produced.get
      } yield assert(records)(equalTo(Vector(new ProducerRecord("topic", Key(42), Value(42)))))
    }
  )
}

sealed class FakeProducer[R, K, V](val produced: Ref[Vector[ProducerRecord[K, V]]]) extends Service[R, K, V] {
  override def produce(record: ProducerRecord[K, V]): RIO[R with Blocking, RecordMetadata]                                = ???
  override def produce(topic: String, key: K, value: V): RIO[R with Blocking, RecordMetadata]                             = ???
  override def produceAsync(record: ProducerRecord[K, V]): RIO[R with Blocking, Task[RecordMetadata]]                     = ???
  override def produceAsync(topic: String, key: K, value: V): RIO[R with Blocking, Task[RecordMetadata]]                  = ???
  override def produceChunkAsync(records: Chunk[ProducerRecord[K, V]]): RIO[R with Blocking, Task[Chunk[RecordMetadata]]] = ???
  override def produceChunk(records: Chunk[ProducerRecord[K, V]]): RIO[R with Blocking, Chunk[RecordMetadata]] =
    produced.update(_ ++ records) *> UIO(Chunk(new RecordMetadata(new TopicPartition("", 0), 0, 0, 0, 0, 0, 0)))
  override def flush: RIO[Blocking, Unit]                      = ???
  override def metrics: RIO[Blocking, Map[MetricName, Metric]] = ???
}
object FakeProducer {
  def mk[R, K, V]: UIO[FakeProducer[R, K, V]] =
    Ref.make(Vector.empty[ProducerRecord[K, V]]).map(new FakeProducer(_))
}
sealed class FailingFakeProducer[R, K, V](override val produced: Ref[Vector[ProducerRecord[K, V]]], counter: Ref[Int])
    extends FakeProducer[R, K, V](produced) {
  override def produceChunkAsync(records: Chunk[ProducerRecord[K, V]]): RIO[R with Blocking, Task[Chunk[RecordMetadata]]] =
    counter.updateAndGet(_ + 1).flatMap {
      case 10 =>
        produced.update(_ ++ records) *> UIO(UIO(Chunk(new RecordMetadata(new TopicPartition("", 0), 0, 0, 0, 0, 0, 0))))
      case n => ZIO.fail(new RuntimeException(s"expected error for testing purposes with counter $n"))
    }
}
object FailingFakeProducer {
  def mk[R, K, V]: UIO[FakeProducer[R, K, V]] = UIO.mapN(Ref.make(Vector.empty[ProducerRecord[K, V]]), Ref.make(0)) {
    new FailingFakeProducer(_, _)
  }
}
