package tamer

import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.producer.ProducerRecord
import tamer.utils.{FailingFakeProducer, FakeProducer}
import zio._
import zio.clock.Clock
import zio.duration.durationInt
import zio.kafka.serde.Serializer
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.environment.TestClock
import zio.test.{DefaultRunnableSpec, assert}

object SinkSpec extends DefaultRunnableSpec {
  val serializerK: Serializer[Any, Key]   = Serializer[Any, Key] { case (_, _, k) => IO(Array(k.key.toByte)) }
  val serializerV: Serializer[Any, Value] = Serializer[Any, Value] { case (_, _, v) => IO(Array(v.value.toByte)) }

  override final val spec = suite("SinkSpec")(
    testM("should correctly produce") {
      for {
        log      <- log4sFromName.provide("test1")
        producer <- FakeProducer.mk[Key, Value](log)
        _ <- Tamer
          .sink(ZStream.repeat(Key(42) -> Value(42)).take(1), producer, "topic", serializerK, serializerV, log)
          .provideSomeLayer[Clock](Registry.fake)
        records <- producer.produced.takeAll
      } yield assert(records)(equalTo(List(new ProducerRecord("topic", Key(42), Value(42)))))
    },
    testM("should correctly produce in case of moderate jitter") {
      for {
        log      <- log4sFromName.provide("test2")
        producer <- FailingFakeProducer.mk[Key, Value](log)
        fiber <- Tamer
          .sink(ZStream.repeat(Key(42) -> Value(42)).take(1), producer, "topic", serializerK, serializerV, log)
          .provideSomeLayer[Clock](Registry.fake)
          .fork
        _       <- TestClock.adjust(5.second).repeat(Schedule.recurs(10))
        _       <- fiber.join
        records <- producer.produced.takeAll
      } yield assert(records)(equalTo(List(new ProducerRecord("topic", Key(42), Value(42)))))
    }
  )
}
