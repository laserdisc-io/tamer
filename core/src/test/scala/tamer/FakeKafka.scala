package tamer

import net.manub.embeddedkafka.schemaregistry.{EmbeddedKWithSR, EmbeddedKafka, EmbeddedKafkaConfig}
import zio._
import zio.duration._
import zio.random.Random

trait FakeKafka {
  def bootstrapServers: List[String]
  def schemaRegistryUrl: String
  def stop(): UIO[Unit]
}

object FakeKafka {

  case class EmbeddedKafkaService(embeddedKWithSR: EmbeddedKWithSR) extends FakeKafka {
    override def bootstrapServers: List[String] = List(s"localhost:${embeddedKWithSR.config.kafkaPort}")
    override def schemaRegistryUrl: String      = s"http://localhost:${embeddedKWithSR.config.schemaRegistryPort}"
    override def stop(): UIO[Unit]              = ZIO.effectTotal(embeddedKWithSR.stop(true))
  }

  case object DefaultLocal extends FakeKafka {
    override def bootstrapServers: List[String] = List(s"localhost:9092")
    override def schemaRegistryUrl: String      = "http://localhost:8081"
    override def stop(): UIO[Unit]              = UIO.unit
  }

  val kafkaConfigLayer: RLayer[Random with Has[FakeKafka], Has[KafkaConfig]] = (for {
    randomString <- random.nextUUID.map(uuid => s"test-$uuid")
    fakeKafka    <- ZIO.service[FakeKafka]
  } yield KafkaConfig(
    brokers = fakeKafka.bootstrapServers,
    schemaRegistryUrl = Some(fakeKafka.schemaRegistryUrl),
    closeTimeout = 1.second,
    bufferSize = 5,
    sink = SinkConfig(s"sink.topic.$randomString"),
    state = StateConfig(s"sink.topic.tape.$randomString", s"embedded.groupid.$randomString", s"embedded.clientid.$randomString")
  )).toLayer

  val embeddedKafkaLayer: TaskLayer[Has[FakeKafka]] = ZLayer.fromManaged {
    implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
      customBrokerProperties = Map("group.min.session.timeout.ms" -> "500", "group.initial.rebalance.delay.ms" -> "0")
    )
    ZManaged.make(ZIO.effect(EmbeddedKafkaService(EmbeddedKafka.start())))(_.stop())
  }

  val embeddedKafkaConfigLayer: RLayer[Random, Has[KafkaConfig]] = embeddedKafkaLayer ++ ZLayer.requires[Random] >>> kafkaConfigLayer

  val localKafkaLayer: ULayer[Has[FakeKafka]] = ZLayer.succeed(DefaultLocal)
}
