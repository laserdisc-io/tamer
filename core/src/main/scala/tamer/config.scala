package tamer

import zio._

final case class RegistryConfig(url: String, cacheSize: Int)
object RegistryConfig {
  def apply(url: String): RegistryConfig = RegistryConfig(
    url = url,
    cacheSize = 1000
  )
  val config: Config[Option[RegistryConfig]] =
    (Config.string("url") ++ Config.int("cache_size").withDefault(1000)).map { case (url, cacheSize) =>
      RegistryConfig(url, cacheSize)
    }.optional
}

final case class SinkConfig(topic: String)
object SinkConfig {
  val config: Config[SinkConfig] = Config.string("topic").map(SinkConfig.apply)
}

final case class StateConfig(topic: String, groupId: String, clientId: String)
object StateConfig {
  val config: Config[StateConfig] =
    (Config.string("topic") ++ Config.string("group_id") ++ Config.string("client_id")).map { case (stateTopic, stateGroupId, stateClientId) =>
      StateConfig(stateTopic, stateGroupId, stateClientId)
    }
}

final case class KafkaConfig(
    brokers: List[String],
    maybeRegistry: Option[RegistryConfig],
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
      maybeRegistry: Option[RegistryConfig],
      closeTimeout: Duration,
      bufferSize: Int,
      sink: SinkConfig,
      state: StateConfig,
      transactionalId: String
  ): KafkaConfig = new KafkaConfig(
    brokers = brokers,
    maybeRegistry = maybeRegistry,
    closeTimeout = closeTimeout,
    bufferSize = bufferSize,
    sink = sink,
    state = state,
    transactionalId = transactionalId,
    properties = Map.empty
  )

  private[this] val kafkaConfigValue = (
    Config.listOf(Config.string("brokers")) ++
      RegistryConfig.config.nested("schema_registry") ++
      Config.duration("close_timeout") ++
      Config.int("buffer_size") ++
      SinkConfig.config.nested("sink") ++
      StateConfig.config.nested("state") ++
      Config.string("transactional_id")
  ).map { case (brokers, maybeRegistry, closeTimeout, bufferSize, sink, state, transactionalId) =>
    KafkaConfig(brokers, maybeRegistry, closeTimeout, bufferSize, sink, state, transactionalId)
  }.nested("kafka")

  final val fromEnvironment: Layer[TamerError, KafkaConfig] = ZLayer {
    ZIO.config(kafkaConfigValue).mapError(ce => TamerError(ce.getMessage(), ce))
  }
}
