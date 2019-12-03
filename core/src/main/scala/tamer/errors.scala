package tamer

sealed abstract class TamerError(msg: String) extends RuntimeException(msg)
final case class ConfigError(msg: String)     extends TamerError(msg)
final case class KafkaError(msg: String)      extends TamerError(msg)
final case class DbError(msg: String)         extends TamerError(msg)
