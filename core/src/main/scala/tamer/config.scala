package tamer

import zio._

final case class SinkConfig(topic: String)
object SinkConfig {
  val config: Config[SinkConfig] = Config.string("kafka_sink_topic").map(SinkConfig.apply)
}

final case class StateConfig(topic: String, groupId: String, clientId: String)
object StateConfig {
  val config: Config[StateConfig] =
    (Config.string("kafka_state_topic") ++ Config.string("kafka_state_group_id") ++ Config.string("kafka_state_client_id")).map {
      case (stateTopic, stateGroupId, stateClientId) => StateConfig(stateTopic, stateGroupId, stateClientId)
    }
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

  private[this] val kafkaConfigValue = (
    Config.listOf(Config.string("kafka_brokers")) ++
      Config.string("kafka_schema_registry_url").optional ++
      Config.duration("kafka_close_timeout") ++
      Config.int("kafka_buffer_size") ++
      SinkConfig.config ++
      StateConfig.config ++
      Config.string("kafka_transactional_id")
  ).map { case (brokers, schemaRegistryUrl, closeTimeout, bufferSize, sink, state, transactionalId) =>
    KafkaConfig(brokers, schemaRegistryUrl, closeTimeout, bufferSize, sink, state, transactionalId)
  }

  final val fromEnvironment: Layer[TamerError, KafkaConfig] = ZLayer {
    ZIO.config(kafkaConfigValue).mapError(ce => TamerError(ce.getMessage(), ce))
  }
}
