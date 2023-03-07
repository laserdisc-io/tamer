package tamer

import io.github.embeddedkafka.schemaregistry.{EmbeddedKWithSR, EmbeddedKafka, EmbeddedKafkaConfig}
import zio._

trait FakeKafka {
  def bootstrapServers: List[String]
  def schemaRegistryUrl: String
  def stop(): UIO[Unit]
}

object FakeKafka {

  case class EmbeddedKafkaService(embeddedKWithSR: EmbeddedKWithSR) extends FakeKafka {
    override def bootstrapServers: List[String] = List(s"localhost:${embeddedKWithSR.config.kafkaPort}")
    override def schemaRegistryUrl: String      = s"http://localhost:${embeddedKWithSR.config.schemaRegistryPort}"
    override def stop(): UIO[Unit]              = ZIO.succeed(embeddedKWithSR.stop(true))
  }

  case object DefaultLocal extends FakeKafka {
    override def bootstrapServers: List[String] = List(s"localhost:9092")
    override def schemaRegistryUrl: String      = "http://localhost:8081"
    override def stop(): UIO[Unit]              = ZIO.unit
  }

  val kafkaConfigLayer: RLayer[FakeKafka, KafkaConfig] = ZLayer {
    for {
      randomString <- Random.nextUUID.map(uuid => s"test-$uuid")
      fakeKafka    <- ZIO.service[FakeKafka]
    } yield KafkaConfig(
      brokers = fakeKafka.bootstrapServers,
      schemaRegistryUrl = Some(fakeKafka.schemaRegistryUrl),
      closeTimeout = 1.second,
      bufferSize = 5,
      sink = SinkConfig(s"sink.topic.$randomString"),
      state = StateConfig(s"sink.topic.tape.$randomString", s"embedded.groupid.$randomString", s"embedded.clientid.$randomString"),
      transactionalId = s"test-transactional-id-$randomString"
    )
  }

  val embeddedKafkaLayer: TaskLayer[FakeKafka] = ZLayer.scoped {
    implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
      customBrokerProperties = Map("group.min.session.timeout.ms" -> "500", "group.initial.rebalance.delay.ms" -> "0")
    )
    ZIO.acquireRelease(ZIO.attempt(EmbeddedKafkaService(EmbeddedKafka.start())))(_.stop())
  }

  val embeddedKafkaConfigLayer: RLayer[Random, KafkaConfig] = embeddedKafkaLayer ++ ZLayer.service[Random] >>> kafkaConfigLayer

  val localKafkaLayer: ULayer[FakeKafka] = ZLayer.succeed(DefaultLocal)
}
