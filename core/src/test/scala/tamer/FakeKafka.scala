package tamer

import io.github.embeddedkafka.schemaregistry.{EmbeddedKWithSR, EmbeddedKafka, EmbeddedKafkaConfig}
import kafka.server.UnboundedControllerMutationQuota
import zio._

trait FakeKafka {
  def bootstrapServers: List[String]
  def schemaRegistryUrl: String
  def createTopic(topic: String): Task[Unit]
  def stop(): UIO[Unit]
}

object FakeKafka {

  case class EmbeddedKafkaService(embeddedKWithSR: EmbeddedKWithSR) extends FakeKafka {
    private[this] final def _createTopic(topic: String) =
      embeddedKWithSR.broker.autoTopicCreationManager.createTopics(Set(topic), UnboundedControllerMutationQuota, None)

    override def bootstrapServers: List[String]         = List(s"localhost:${embeddedKWithSR.config.kafkaPort}")
    override def schemaRegistryUrl: String              = s"http://localhost:${embeddedKWithSR.config.schemaRegistryPort}"
    override def createTopic(topic: String): Task[Unit] = ZIO.attemptBlocking(_createTopic(topic)).unit
    override def stop(): UIO[Unit]                      = ZIO.attemptBlocking(embeddedKWithSR.stop(true)).ignore
  }

  case object DefaultLocal extends FakeKafka {
    override def bootstrapServers: List[String]         = List("localhost:9092")
    override def schemaRegistryUrl: String              = "http://localhost:8081"
    override def createTopic(topic: String): Task[Unit] = ZIO.unit
    override def stop(): UIO[Unit]                      = ZIO.unit
  }

  val kafkaConfigLayer: RLayer[FakeKafka, KafkaConfig] = ZLayer {
    for {
      randomString <- Random.nextUUID.map(uuid => s"test-$uuid")
      fakeKafka    <- ZIO.service[FakeKafka]
      _            <- fakeKafka.createTopic(s"sink.topic.$randomString")
      _            <- fakeKafka.createTopic(s"state.topic.$randomString")
    } yield KafkaConfig(
      brokers = fakeKafka.bootstrapServers,
      maybeRegistry = Some(RegistryConfig(fakeKafka.schemaRegistryUrl)),
      closeTimeout = 1.second,
      bufferSize = 5,
      sink = TopicConfig(s"sink.topic.$randomString"),
      state = TopicConfig(s"state.topic.$randomString"),
      groupId = s"groupid.$randomString",
      clientId = s"clientid.$randomString",
      transactionalId = s"transactionalid.$randomString"
    )
  }

  val embeddedKafkaLayer: TaskLayer[FakeKafka] = ZLayer.scoped {
    implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
      customBrokerProperties = Map("group.min.session.timeout.ms" -> "500", "group.initial.rebalance.delay.ms" -> "0")
    )
    ZIO.acquireRelease(ZIO.attempt(EmbeddedKafkaService(EmbeddedKafka.start())))(_.stop())
  }

  val embeddedKafkaConfigLayer: TaskLayer[KafkaConfig] = embeddedKafkaLayer >>> kafkaConfigLayer

  val localKafkaLayer: ULayer[FakeKafka] = ZLayer.succeed(DefaultLocal)
}
