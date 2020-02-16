package tamer
package config

import cats.implicits._
import ciris.{ConfigError => CirisConfigError, _}
import ciris.refined._
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import zio.{IO, Task, ZIO}
import zio.interop.catz._
import zio.macros.annotation.accessible

import scala.concurrent.duration.FiniteDuration

final case class DbConfig(driver: NonEmptyString, uri: UriString, username: NonEmptyString, password: Password)
final case class KafkaSinkConfig(topic: NonEmptyString)
final case class KafkaStateConfig(topic: NonEmptyString, groupId: NonEmptyString, clientId: NonEmptyString)
final case class KafkaConfig(
    brokers: HostList,
    schemaRegistryUrl: UrlString,
    closeTimeout: FiniteDuration,
    bufferSize: PosInt,
    sink: KafkaSinkConfig,
    state: KafkaStateConfig
)
final case class TamerConfig(db: DbConfig, kafka: KafkaConfig)

@accessible(">") trait Config extends Serializable {
  val config: Config.Service[Any]
}

object Config {
  trait Service[R] {
    val load: ZIO[R, ConfigError, TamerConfig]
  }

  trait Live extends Config {
    private[this] implicit final val hostListConfigDecoder: ConfigDecoder[String, HostList] =
      ConfigDecoder.identity[String].map(_.split(",").toList.map(_.trim)).mapEither(ConfigDecoder[List[String], HostList].decode)

    override final val config: Service[Any] = new Service[Any] {
      private val dbConfig = (
        env("DATABASE_DRIVER").as[NonEmptyString],
        env("DATABASE_URL").as[UriString],
        env("DATABASE_USERNAME").as[NonEmptyString],
        env("DATABASE_PASSWORD").as[Password].redacted
      ).parMapN(DbConfig)

      private val kafkaSinkConfig = env("KAFKA_SINK_TOPIC").as[NonEmptyString].map(KafkaSinkConfig)
      private val kafkaStateConfig = (
        env("KAFKA_STATE_TOPIC").as[NonEmptyString],
        env("KAFKA_STATE_GROUP_ID").as[NonEmptyString],
        env("KAFKA_STATE_CLIENT_ID").as[NonEmptyString]
      ).parMapN(KafkaStateConfig)
      private val kafkaConfig = (
        env("KAFKA_BROKERS").as[HostList],
        env("KAFKA_SCHEMA_REGISTRY_URL").as[UrlString],
        env("KAFKA_CLOSE_TIMEOUT").as[FiniteDuration],
        env("KAFKA_BUFFER_SIZE").as[PosInt],
        kafkaSinkConfig,
        kafkaStateConfig
      ).parMapN(KafkaConfig)

      val tamerConfig: ConfigValue[TamerConfig] = (dbConfig, kafkaConfig).parMapN { (db, kafka) => TamerConfig(db, kafka) }

      override final val load: IO[ConfigError, TamerConfig] =
        tamerConfig.load[Task].refineToOrDie[CirisConfigError].mapError(ce => ConfigError(ce.redacted.show))
    }
  }

  object Live extends Live
}
