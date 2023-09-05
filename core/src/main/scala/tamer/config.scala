package tamer

import zio.{Duration, Layer}
import zio.config.{ConfigDescriptor, ZConfig}

final case class SinkConfig(topic: String)

object SinkConfig {
  val configDescriptor: ConfigDescriptor[SinkConfig] = ConfigDescriptor.string("KAFKA_SINK_TOPIC").to[SinkConfig]
}

final case class StateConfig(topic: String, groupId: String, clientId: String)

object StateConfig {
  val configDescriptor: ConfigDescriptor[StateConfig] =
    (
      ConfigDescriptor.string("KAFKA_STATE_TOPIC") zip
        ConfigDescriptor.string("KAFKA_STATE_GROUP_ID") zip
        ConfigDescriptor.string("KAFKA_STATE_CLIENT_ID")
    ).to[StateConfig]
}

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

  val configDescriptor: ConfigDescriptor[KafkaConfig] =
    (
      ConfigDescriptor.list("KAFKA_BROKERS")(ConfigDescriptor.string) zip
        ConfigDescriptor.string("KAFKA_SCHEMA_REGISTRY_URL").optional zip
        ConfigDescriptor.duration("KAFKA_CLOSE_TIMEOUT").map(Duration.fromScala) zip
        ConfigDescriptor.int("KAFKA_BUFFER_SIZE") zip
        SinkConfig.configDescriptor zip
        StateConfig.configDescriptor zip
        ConfigDescriptor.string("KAFKA_TRANSACTIONAL_ID")
    ).transform(
      { case ((brokers, schemaRegistryUrl, closeTimeout, bufferSize, sink, state, transactionalId)) =>
        KafkaConfig(brokers, schemaRegistryUrl, closeTimeout, bufferSize, sink, state, transactionalId)
      },
      a => (a.brokers, a.schemaRegistryUrl, a.closeTimeout, a.bufferSize, a.sink, a.state, a.transactionalId)
    )

  final val fromEnvironment: Layer[TamerError, KafkaConfig] = ZConfig.fromSystemEnv(configDescriptor).mapError(e => TamerError(e.prettyPrint(), e))
}
