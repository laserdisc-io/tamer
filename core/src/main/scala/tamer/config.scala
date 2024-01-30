package tamer

import java.time.{Duration => JDuration}

import zio._

final case class RegistryConfig(url: String, cacheSize: Int, expiration: JDuration)
object RegistryConfig {
  def apply(url: String): RegistryConfig = RegistryConfig(
    url = url,
    cacheSize = 4,
    expiration = 1.hour
  )
  val config: Config[Option[RegistryConfig]] =
    (Config.string("url") ++ Config.int("cache_size").withDefault(4) ++ Config.duration("expiration").withDefault(1.hour)).map {
      case (url, cacheSize, expiration) => RegistryConfig(url, cacheSize, expiration)
    }.optional
}

final case class TopicOptions(partitions: Int, replicas: Short)
object TopicOptions {
  val config: Config[Option[TopicOptions]] =
    (Config.boolean("auto_create").withDefault(false) ++
      Config.int("partitions").withDefault(1) ++
      Config.int("replicas").map(_.toShort).withDefault(1.toShort)).map {
      case (true, partitions, replicas) => Some(TopicOptions(partitions, replicas))
      case _                            => None
    }
}

final case class TopicConfig(topicName: String, maybeTopicOptions: Option[TopicOptions])
object TopicConfig {
  def apply(topicName: String): TopicConfig = new TopicConfig(
    topicName = topicName,
    maybeTopicOptions = None
  )
  val config: Config[TopicConfig] = (Config.string("topic") ++ TopicOptions.config).map { case (topicName, maybeTopicOptions) =>
    TopicConfig(topicName, maybeTopicOptions)
  }
}

final case class KafkaConfig(
    brokers: List[String],
    maybeRegistry: Option[RegistryConfig],
    closeTimeout: Duration,
    bufferSize: Int,
    sink: TopicConfig,
    state: TopicConfig,
    groupId: String,
    clientId: String,
    transactionalId: String,
    properties: Map[String, AnyRef]
)
object KafkaConfig {
  def apply(
      brokers: List[String],
      maybeRegistry: Option[RegistryConfig],
      closeTimeout: Duration,
      bufferSize: Int,
      sink: TopicConfig,
      state: TopicConfig,
      groupId: String,
      clientId: String,
      transactionalId: String
  ): KafkaConfig = new KafkaConfig(
    brokers = brokers,
    maybeRegistry = maybeRegistry,
    closeTimeout = closeTimeout,
    bufferSize = bufferSize,
    sink = sink,
    state = state,
    groupId = groupId,
    clientId = clientId,
    transactionalId = transactionalId,
    properties = Map.empty
  )

  private[this] val kafkaConfigValue = (
    Config.listOf(Config.string("brokers")) ++
      RegistryConfig.config.nested("schema_registry") ++
      Config.duration("close_timeout") ++
      Config.int("buffer_size") ++
      TopicConfig.config.nested("sink") ++
      TopicConfig.config.nested("state") ++
      Config.string("group_id") ++
      Config.string("client_id") ++
      Config.string("transactional_id")
  ).map { case (brokers, maybeRegistry, closeTimeout, bufferSize, sink, state, groupId, clientId, transactionalId) =>
    KafkaConfig(brokers, maybeRegistry, closeTimeout, bufferSize, sink, state, groupId, clientId, transactionalId)
  }.nested("kafka")

  final val fromEnvironment: TaskLayer[KafkaConfig] = ZLayer {
    ZIO.config(kafkaConfigValue).mapError(ce => TamerError(ce.getMessage(), ce))
  }
}
