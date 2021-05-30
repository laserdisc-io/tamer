package tamer

import eu.timepit.refined.api.RefType
import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.NonEmptyString
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKWithSR, EmbeddedKafka, EmbeddedKafkaConfig}
import zio._

import scala.concurrent.duration._

trait FakeKafka {
  def bootstrapServers: HostList
  def schemaRegistryUrl: UrlString
  def stop(): UIO[Unit]
}

object FakeKafka {

  case class EmbeddedKafkaService(embeddedKWithSR: EmbeddedKWithSR) extends FakeKafka {
    override def bootstrapServers: HostList =
      RefType.applyRef[HostList].unsafeFrom(List(s"localhost:${embeddedKWithSR.config.kafkaPort}"))
    override def schemaRegistryUrl: UrlString =
      RefType.applyRef[UrlString].unsafeFrom(s"http://localhost:${embeddedKWithSR.config.schemaRegistryPort}")
    override def stop(): UIO[Unit] = ZIO.effectTotal(embeddedKWithSR.stop(true))
  }

  case object DefaultLocal extends FakeKafka {
    override def bootstrapServers: HostList   = RefType.applyRef[HostList].unsafeFrom(List(s"localhost:9092"))
    override def schemaRegistryUrl: UrlString = "http://localhost:8081"
    override def stop(): UIO[Unit]            = UIO.unit
  }

  val embeddedKafkaConfig: ZLayer[Has[FakeKafka], Throwable, Has[KafkaConfig]] = (for {
    randomString <- TestUtils.randomThing("test")
    fakeKafka    <- ZIO.service[FakeKafka]
  } yield KafkaConfig(
    brokers = fakeKafka.bootstrapServers,
    schemaRegistryUrl = fakeKafka.schemaRegistryUrl,
    closeTimeout = 1.second,
    bufferSize = 5,
    sink = SinkConfig(RefType.applyRef[NonEmptyString].unsafeFrom(s"sink.topic.$randomString")),
    state = StateConfig(
      RefType.applyRef[NonEmptyString].unsafeFrom(s"sink.topic.tape.$randomString"),
      RefType.applyRef[NonEmptyString].unsafeFrom(s"embedded.groupid.$randomString"),
      RefType.applyRef[NonEmptyString].unsafeFrom(s"embedded.clientid.$randomString")
    )
  )).toLayer

  val embedded: TaskLayer[Has[FakeKafka]] = ZLayer.fromManaged {
    implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
      customBrokerProperties = Map("group.min.session.timeout.ms" -> "500", "group.initial.rebalance.delay.ms" -> "0")
    )
    ZManaged.make(ZIO.effect(EmbeddedKafkaService(EmbeddedKafka.start())))(_.stop())
  }

  val local: ULayer[Has[FakeKafka]] = ZLayer.succeed(DefaultLocal)
}
