package tamer

import cats.implicits._
import ciris._
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.interop.catz._

final case class SinkConfig(topic: String)
final case class StateConfig(topic: String, groupId: String, clientId: String)
final case class KafkaConfig(
    brokers: List[String],
    schemaRegistryUrl: Option[String],
    closeTimeout: Duration,
    bufferSize: Int,
    sink: SinkConfig,
    state: StateConfig,
    transactionalId: String,
    properties: Map[String, AnyRef]
)

object KafkaConfig {
  def apply(
      brokers: List[String],
      schemaRegistryUrl: Option[String],
      closeTimeout: Duration,
      bufferSize: Int,
      sink: SinkConfig,
      state: StateConfig,
      transactionalId: String
  ): KafkaConfig = new KafkaConfig(
    brokers = brokers,
    schemaRegistryUrl = schemaRegistryUrl,
    closeTimeout = closeTimeout,
    bufferSize = bufferSize,
    sink = sink,
    state = state,
    transactionalId = transactionalId,
    properties = Map.empty
  )

  private[this] implicit final val durationConfigDecoder: ConfigDecoder[String, Duration] =
    ConfigDecoder.stringFiniteDurationConfigDecoder.map(Duration.fromScala)
  private[this] implicit final val hostListConfigDecoder: ConfigDecoder[String, List[String]] =
    ConfigDecoder.identity[String].map(_.split(",").toList.map(_.trim))

  private[this] val kafkaSinkConfigValue = env("KAFKA_SINK_TOPIC").map(SinkConfig)
  private[this] val kafkaStateConfigValue =
    (env("KAFKA_STATE_TOPIC"), env("KAFKA_STATE_GROUP_ID"), env("KAFKA_STATE_CLIENT_ID")).mapN(StateConfig)
  private[this] val kafkaConfigValue = (
    env("KAFKA_BROKERS").as[List[String]],
    env("KAFKA_SCHEMA_REGISTRY_URL").option,
    env("KAFKA_CLOSE_TIMEOUT").as[Duration],
    env("KAFKA_BUFFER_SIZE").as[Int],
    kafkaSinkConfigValue,
    kafkaStateConfigValue,
    env("KAFKA_TRANSACTIONAL_ID").as[String]
  ).mapN(KafkaConfig.apply)

  final val fromEnvironment: ZLayer[Blocking with Clock, TamerError, Has[KafkaConfig]] =
    ZIO
      .runtime[Clock with Blocking]
      .flatMap(implicit runtime => kafkaConfigValue.load[Task])
      .refineToOrDie[ConfigException]
      .mapError(ce => TamerError(ce.error.redacted.show, ce))
      .toLayer
}
