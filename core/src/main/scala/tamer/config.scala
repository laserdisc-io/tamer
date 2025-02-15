/*
 * Copyright (c) 2019-2025 LaserDisc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package tamer

import java.nio.charset.StandardCharsets
import java.time.{Duration => JDuration}
import java.util.Base64

import zio._

sealed trait RegistryAuthConfig extends Product with Serializable
object RegistryAuthConfig {
  final case class Basic(userInfo: String) extends RegistryAuthConfig
  object Basic {
    def apply(userInfo: String): Basic                   = new Basic(Base64.getEncoder.encodeToString(userInfo.getBytes(StandardCharsets.UTF_8)))
    def apply(username: String, password: String): Basic = new Basic(s"$username:$password")
  }
  final case class Bearer(token: String) extends RegistryAuthConfig
  val config: Config[Option[RegistryAuthConfig]] =
    (Config.secret("user_info").optional ++
      Config.secret("username").optional ++
      Config.secret("password").optional ++
      Config.secret("token").optional).mapOrFail {
      case (None, None, None, None)                     => Right(None)
      case (Some(userInfo), None, None, None)           => Right(Some(Basic(userInfo.value.asString)))
      case (None, Some(username), Some(password), None) => Right(Some(Basic(username.value.asString, password.value.asString)))
      case (None, None, None, Some(token))              => Right(Some(Bearer(token.value.asString)))
      case _ =>
        Left(
          Config.Error.InvalidData(message =
            "When auth is configured you must specify one of these three options (mutually exclusive): user_info (Basic auth), username and password pair (Basic auth) or token (Bearer auth)"
          )
        )
    }
}

final case class RegistryConfig(url: String, cacheSize: Int, expiration: JDuration, maybeRegistryAuth: Option[RegistryAuthConfig])
object RegistryConfig {
  def apply(url: String): RegistryConfig = RegistryConfig(
    url = url,
    cacheSize = 4,
    expiration = 1.hour,
    maybeRegistryAuth = None
  )
  val config: Config[Option[RegistryConfig]] =
    (Config.string("url") ++
      Config.int("cache_size").withDefault(4) ++
      Config.duration("expiration").withDefault(1.hour) ++
      RegistryAuthConfig.config.nested("auth")).map { case (url, cacheSize, expiration, maybeRegistryAuth) =>
      RegistryConfig(url, cacheSize, expiration, maybeRegistryAuth)
    }.optional
}

final case class TopicOptions(partitions: Int, replicas: Short, compaction: Boolean)
object TopicOptions {
  def config(compactionDefault: Boolean): Config[Option[TopicOptions]] =
    (Config.boolean("auto_create").withDefault(false) ++
      Config.int("partitions").withDefault(1) ++
      Config.int("replicas").map(_.toShort).withDefault(1.toShort) ++
      Config.boolean("compaction").withDefault(compactionDefault)).map {
      case (true, partitions, replicas, compaction) => Some(TopicOptions(partitions, replicas, compaction))
      case _                                        => None
    }
}

final case class TopicConfig(topicName: String, maybeTopicOptions: Option[TopicOptions])
object TopicConfig {
  def apply(topicName: String): TopicConfig = new TopicConfig(
    topicName = topicName,
    maybeTopicOptions = None
  )
  def config(compactionDefault: Boolean): Config[TopicConfig] =
    (Config.string("topic") ++ TopicOptions.config(compactionDefault)).map { case (topicName, maybeTopicOptions) =>
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
      TopicConfig.config(compactionDefault = false).nested("sink") ++
      TopicConfig.config(compactionDefault = true).nested("state") ++
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
