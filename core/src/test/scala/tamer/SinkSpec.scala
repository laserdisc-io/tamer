package tamer

import io.confluent.kafka.schemaregistry.ParsedSchema
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.producer.ProducerRecord
import utils.{FailingFakeProducer, FakeProducer}
import zio._
import zio.duration.durationInt
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
        producer <- FakeProducer.mk[RegistryInfo, Key, Value](log)
        _        <- Tamer.sink(ZStream.repeat(Key(42) -> Value(42)).take(1), producer, "topic", nullRegistryInfoFor("topic"), log)
        records  <- producer.produced.takeAll
      } yield assert(records)(equalTo(List(new ProducerRecord("topic", Key(42), Value(42)))))
    },
    testM("should correctly produce in case of moderate jitter") {
      for {
        log      <- log4sFromName.provide("test2")
        producer <- FailingFakeProducer.mk[RegistryInfo, Key, Value](log)
        fiber    <- Tamer.sink(ZStream.repeat(Key(42) -> Value(42)).take(1), producer, "topic", nullRegistryInfoFor("topic"), log).fork
        _        <- TestClock.adjust(5.second).repeat(Schedule.recurs(10))
        _        <- fiber.join
        records  <- producer.produced.takeAll
      } yield assert(records)(equalTo(List(new ProducerRecord("topic", Key(42), Value(42)))))
    }
  )
}
