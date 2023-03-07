import zio.{Layer, URIO, ZIO}

package object tamer {
  final val kafkaConfig: URIO[KafkaConfig, KafkaConfig] = ZIO.service
  final val runLoop: ZIO[Tamer, TamerError, Unit]       = ZIO.serviceWithZIO(_.runLoop)

  implicit final class HashableOps[A](private val _underlying: A) extends AnyVal {
    def hash(implicit A: Hashable[A]): Int = A.hash(_underlying)
  }

  final val kafkaConfigFromEnvironment: Layer[TamerError, KafkaConfig] = KafkaConfig.fromEnvironment
}
