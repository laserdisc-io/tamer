package tamer
package kafka

import cats.implicits._
import ciris._
import ciris.refined._
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import zio._
import zio.interop.catz._

import scala.concurrent.duration.FiniteDuration

final case class SinkConfig(topic: NonEmptyString)
final case class StateConfig(topic: NonEmptyString, groupId: NonEmptyString, clientId: NonEmptyString)
final case class KafkaConfig(
    brokers: HostList,
    schemaRegistryUrl: UrlString,
    closeTimeout: FiniteDuration,
    bufferSize: PosInt,
    sink: SinkConfig,
    state: StateConfig,
    properties: Map[String, AnyRef]
)

object KafkaConfig {
  def apply(
      brokers: HostList,
      schemaRegistryUrl: UrlString,
      closeTimeout: FiniteDuration,
      bufferSize: PosInt,
      sink: SinkConfig,
      state: StateConfig
  ): KafkaConfig = new KafkaConfig(
    brokers = brokers,
    schemaRegistryUrl = schemaRegistryUrl,
    closeTimeout = closeTimeout,
    bufferSize = bufferSize,
    sink = sink,
    state = state,
    properties = Map.empty
  )

  private[this] implicit final val hostListConfigDecoder: ConfigDecoder[String, HostList] =
    ConfigDecoder.identity[String].map(_.split(",").toList.map(_.trim)).mapEither(ConfigDecoder[List[String], HostList].decode)

  private[this] val kafkaSinkConfigValue = env("KAFKA_SINK_TOPIC").as[NonEmptyString].map(SinkConfig)
  private[this] val kafkaStateConfigValue = (
    env("KAFKA_STATE_TOPIC").as[NonEmptyString],
    env("KAFKA_STATE_GROUP_ID").as[NonEmptyString],
    env("KAFKA_STATE_CLIENT_ID").as[NonEmptyString]
  ).parMapN(StateConfig)
  private[this] val kafkaConfigValue = (
    env("KAFKA_BROKERS").as[HostList],
    env("KAFKA_SCHEMA_REGISTRY_URL").as[UrlString],
    env("KAFKA_CLOSE_TIMEOUT").as[FiniteDuration],
    env("KAFKA_BUFFER_SIZE").as[PosInt],
    kafkaSinkConfigValue,
    kafkaStateConfigValue
  ).parMapN(KafkaConfig.apply)

  lazy val fromEnvironment: Layer[TamerError, Has[KafkaConfig]] = ZLayer.fromEffect {
    kafkaConfigValue.load[Task].refineToOrDie[ConfigException].mapError(ce => TamerError(ce.error.redacted.show, ce))
  }
}
