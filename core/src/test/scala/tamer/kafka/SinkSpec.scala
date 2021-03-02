package tamer.kafka

import eu.timepit.refined.auto._
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import zio.blocking.Blocking
import zio.duration.durationInt
import zio.kafka.producer.Producer.Service
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.environment.{TestClock, TestEnvironment}
import zio.test.{DefaultRunnableSpec, ZSpec, assert}
import zio.{Chunk, RIO, Ref, Schedule, Task, UIO, ZIO}

case class Key(key: Int)
case class Value(value: Int)
case class State(i: Int)
object SinkSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[TestEnvironment, Any] =
    suite("SinkSpec")(
      testM("should correctly produce ") {
        val fakeValueProducer = Test.mk[Any, Key, Value]
        val fakeInputStream   = ZStream.repeat((Key(42), Value(42))).take(1)
        for {
          log                 <- log4sFromName.provide("test1")
          fakeProducerService <- fakeValueProducer
          _                   <- Kafka.sink(fakeInputStream, fakeProducerService, "topic", log)
          producedRecords     <- fakeProducerService.producedValues.get
        } yield assert(producedRecords)(contains(new ProducerRecord("topic", Key(42), Value(42))))
      },
      testM("should correctly produce in case of moderate jitter") {
        val fakeFailingValueProducer = Test.mkFailing[Any, Key, Value]
        val fakeInputStream          = ZStream.repeat((Key(42), Value(42))).take(1)
        for {
          log                 <- log4sFromName.provide("test2")
          fakeProducerService <- fakeFailingValueProducer
          fiber               <- Kafka.sink(fakeInputStream, fakeProducerService, "topic", log).fork
          _                   <- TestClock.adjust(5.second).repeat(Schedule.recurs(10))
          _                   <- fiber.join
          producedRecords     <- fakeProducerService.producedValues.get
        } yield assert(producedRecords)(contains(new ProducerRecord("topic", Key(42), Value(42))))
      }
    )
}

class Test[R, K, V](val producedValues: Ref[Vector[ProducerRecord[K, V]]]) extends Service[R, K, V] {
  override def produce(record: ProducerRecord[K, V]): RIO[R with Blocking, RecordMetadata]               = ???
  override def produce(topic: String, key: K, value: V): RIO[R with Blocking, RecordMetadata]            = ???
  override def produceAsync(record: ProducerRecord[K, V]): RIO[R with Blocking, Task[RecordMetadata]]    = ???
  override def produceAsync(topic: String, key: K, value: V): RIO[R with Blocking, Task[RecordMetadata]] = ???
  override def produceChunkAsync(records: Chunk[ProducerRecord[K, V]]): RIO[R with Blocking, Task[Chunk[RecordMetadata]]] =
    producedValues.update(_.appendedAll(records)) *> UIO(UIO(Chunk(new RecordMetadata(new TopicPartition("", 0), 0, 0, 0, 0, 0, 0))))
  override def produceChunk(records: Chunk[ProducerRecord[K, V]]): RIO[R with Blocking, Chunk[RecordMetadata]] = ???
  override def flush: RIO[Blocking, Unit]                                                                      = ???
  override def metrics: RIO[Blocking, Map[MetricName, Metric]]                                                 = ???
}
object Test {
  def mk[R, K, V]: UIO[Test[R, K, V]] =
    for {
      producedValues <- Ref.make(Vector.empty[ProducerRecord[K, V]])
    } yield new Test(producedValues)

  def mkFailing[R, K, V]: UIO[Test[R, K, V]] =
    for {
      counter        <- Ref.make(0)
      producedValues <- Ref.make(Vector.empty[ProducerRecord[K, V]])
    } yield new FailingTest(producedValues, counter)
}

class FailingTest[R, K, V](override val producedValues: Ref[Vector[ProducerRecord[K, V]]], counter: Ref[Int]) extends Test[R, K, V](producedValues) {
  override def produceChunkAsync(records: Chunk[ProducerRecord[K, V]]): RIO[R with Blocking, Task[Chunk[RecordMetadata]]] =
    counter.updateAndGet(_ + 1).flatMap {
      case 10 =>
        producedValues.update(vals => vals.appendedAll(records)) *> UIO(UIO(Chunk(new RecordMetadata(new TopicPartition("", 0), 0, 0, 0, 0, 0, 0))))
      case n => ZIO.fail(new RuntimeException(s"expected error for testing purposes with counter $n"))
    }
}
