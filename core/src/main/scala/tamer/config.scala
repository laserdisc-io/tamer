package tamer

import cats.implicits._
import ciris._
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.interop.catz._

sealed trait StateRecoveryStrategy extends Product with Serializable
case object ManualRecovery         extends StateRecoveryStrategy
case object AutomaticRecovery      extends StateRecoveryStrategy

final case class SinkConfig(topic: String)
final case class StateConfig(topic: String, groupId: String, clientId: String, recoveryStrategy: StateRecoveryStrategy = ManualRecovery)
final case class KafkaConfig(
    brokers: List[String],
    schemaRegistryUrl: Option[String],
    closeTimeout: Duration,
    bufferSize: Int,
    sink: SinkConfig,
    state: StateConfig,
    properties: Map[String, AnyRef]
)

object KafkaConfig {
  def apply(
      brokers: List[String],
      schemaRegistryUrl: Option[String],
      closeTimeout: Duration,
      bufferSize: Int,
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

  private[this] implicit final val durationConfigDecoder: ConfigDecoder[String, Duration] =
    ConfigDecoder.stringFiniteDurationConfigDecoder.map(Duration.fromScala)
  private[this] implicit final val hostListConfigDecoder: ConfigDecoder[String, List[String]] =
    ConfigDecoder.identity[String].map(_.split(",").toList.map(_.trim))

  private[this] val kafkaSinkConfigValue = env("KAFKA_SINK_TOPIC").map(SinkConfig)
  private[this] val kafkaStateConfigValue =
    (env("KAFKA_STATE_TOPIC"), env("KAFKA_STATE_GROUP_ID"), env("KAFKA_STATE_CLIENT_ID")).mapN(StateConfig(_, _, _, ManualRecovery))
  private[this] val kafkaConfigValue = (
    env("KAFKA_BROKERS").as[List[String]],
    env("KAFKA_SCHEMA_REGISTRY_URL").option,
    env("KAFKA_CLOSE_TIMEOUT").as[Duration],
    env("KAFKA_BUFFER_SIZE").as[Int],
    kafkaSinkConfigValue,
    kafkaStateConfigValue
  ).mapN(KafkaConfig.apply)

  final val fromEnvironment: ZLayer[Blocking with Clock, TamerError, Has[KafkaConfig]] =
    ZIO
      .runtime[Clock with Blocking]
      .flatMap(implicit runtime => kafkaConfigValue.load[Task])
      .refineToOrDie[ConfigException]
      .mapError(ce => TamerError(ce.error.redacted.show, ce))
      .toLayer
}
