package tamer
package config

import cats.implicits._
import ciris._
import ciris.refined._
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import zio._
import zio.interop.catz._

import scala.concurrent.duration.FiniteDuration

object Config {
  final case class KafkaSink(topic: NonEmptyString)
  final case class KafkaState(topic: NonEmptyString, groupId: NonEmptyString, clientId: NonEmptyString)
  final case class Kafka(
      brokers: HostList,
      schemaRegistryUrl: UrlString,
      closeTimeout: FiniteDuration,
      bufferSize: PosInt,
      sink: KafkaSink,
      state: KafkaState
  )

  private[this] implicit final val hostListConfigDecoder: ConfigDecoder[String, HostList] =
    ConfigDecoder.identity[String].map(_.split(",").toList.map(_.trim)).mapEither(ConfigDecoder[List[String], HostList].decode)

  private[this] val kafkaSinkConfigValue = env("KAFKA_SINK_TOPIC").as[NonEmptyString].map(KafkaSink)
  private[this] val kafkaStateConfigValue = (
    env("KAFKA_STATE_TOPIC").as[NonEmptyString],
    env("KAFKA_STATE_GROUP_ID").as[NonEmptyString],
    env("KAFKA_STATE_CLIENT_ID").as[NonEmptyString]
  ).parMapN(KafkaState)
  private[this] val kafkaConfigValue = (
    env("KAFKA_BROKERS").as[HostList],
    env("KAFKA_SCHEMA_REGISTRY_URL").as[UrlString],
    env("KAFKA_CLOSE_TIMEOUT").as[FiniteDuration],
    env("KAFKA_BUFFER_SIZE").as[PosInt],
    kafkaSinkConfigValue,
    kafkaStateConfigValue
  ).parMapN(Kafka)

  trait Service {
    val kafkaConfig: URIO[KafkaConfig, Kafka]
  }

  val live: Layer[TamerError, KafkaConfig] = ZLayer.fromEffect {
    kafkaConfigValue.load[Task].refineToOrDie[ConfigException].mapError(ce => TamerError(ce.error.redacted.show, ce))
  }
}
