package tamer

sealed abstract class TamerError(msg: String, cause: Throwable)    extends RuntimeException(msg, cause)
final case class ConfigError(msg: String, cause: Throwable)        extends TamerError(msg, cause)
final case class KafkaError(msg: String, cause: Throwable)         extends TamerError(msg, cause)
final case class DbError(msg: String, cause: Throwable)            extends TamerError(msg, cause)
final case class SerializationError(msg: String, cause: Throwable) extends TamerError(msg, cause)
final case class SetupError(msg: String, cause: Throwable)         extends TamerError(msg, cause)

object ConfigError        { final def apply(msg: String): ConfigError = new ConfigError(msg, null)               }
object KafkaError         { final def apply(msg: String): KafkaError = new KafkaError(msg, null)                 }
object DbError            { final def apply(msg: String): DbError = new DbError(msg, null)                       }
object SerializationError { final def apply(msg: String): SerializationError = new SerializationError(msg, null) }
object SetupError         { final def apply(msg: String): SetupError = new SetupError(msg, null)                 }
