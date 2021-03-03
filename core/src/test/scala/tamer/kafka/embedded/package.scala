package tamer.kafka

import net.manub.embeddedkafka.schemaregistry.{EmbeddedKWithSR, EmbeddedKafka, EmbeddedKafkaConfig}
import tamer.config.{Config, HostList, KafkaConfig, UrlString}
import eu.timepit.refined.auto._
import eu.timepit.refined.boolean.{And, Or}
import eu.timepit.refined.collection.{Forall, NonEmpty}
import eu.timepit.refined.refineV
import eu.timepit.refined.string.{IPv4, Uri, Url}
import tamer.config.Config.{KafkaSink, KafkaState}
import zio._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

package object embedded {
  type KafkaTest = Has[KafkaTest.Service]

  object KafkaTest {
    trait Service {
      def bootstrapServers: HostList
      def schemaRegistryUrl: UrlString
      def stop(): UIO[Unit]
    }

    case class EmbeddedKafkaService(embeddedKWithSR: EmbeddedKWithSR) extends Service {
      override def bootstrapServers: HostList =
        refineV[NonEmpty And Forall[IPv4 Or Uri]].unsafeFrom(List(s"localhost:${embeddedKWithSR.config.kafkaPort}"))
      override def schemaRegistryUrl: UrlString = refineV[Url].unsafeFrom(s"http://localhost:${embeddedKWithSR.config.schemaRegistryPort}")
      override def stop(): UIO[Unit]            = ZIO.effectTotal(embeddedKWithSR.stop(true))
    }

    case object DefaultLocal extends Service {
      override def bootstrapServers: HostList   = refineV[NonEmpty And Forall[IPv4 Or Uri]].unsafeFrom(List(s"localhost:9092"))
      override def schemaRegistryUrl: UrlString = "http://localhost:8081"
      override def stop(): UIO[Unit]            = UIO.unit
    }

    val embeddedKafkaConfig: ZLayer[KafkaTest, Throwable, KafkaConfig] = ZIO
      .service[KafkaTest.Service]
      .map(heks =>
        Config.Kafka(
          brokers = heks.bootstrapServers,
          schemaRegistryUrl = heks.schemaRegistryUrl,
          closeTimeout = FiniteDuration(1, TimeUnit.SECONDS),
          bufferSize = 5,
          sink = KafkaSink("sink.topic"),
          state = KafkaState("sink.topic.tape", "embedded.groupid", "embedded.clientid")
        )
      )
      .toLayer

    val embeddedKafkaTest: ZLayer[Any, Throwable, KafkaTest] = ZLayer
      .fromManaged {
        implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
          customBrokerProperties = Map("group.min.session.timeout.ms" -> "500", "group.initial.rebalance.delay.ms" -> "0")
        )
        ZManaged.make(ZIO.effect(EmbeddedKafkaService(EmbeddedKafka.start())))(_.stop())
      }

    val local: ZLayer[Any, Nothing, KafkaTest] = ZLayer.succeed(DefaultLocal)
  }
}
