import zio.{Has, Layer, URIO, ZIO, ZLayer}
import zio.clock.Clock

package object tamer {
  final val kafkaConfig: URIO[Has[KafkaConfig], KafkaConfig]                         = ZIO.service
  final val runLoop: ZIO[Has[Tamer] with Clock with Has[Registry], TamerError, Unit] = ZIO.accessM(_.get.runLoop)

  implicit final class HashableOps[A](private val _underlying: A) extends AnyVal {
    def hash(implicit A: Hashable[A]): Int = A.hash(_underlying)
  }

  final val kafkaConfigFromEnvironment: Layer[TamerError, Has[KafkaConfig]]              = KafkaConfig.fromEnvironment
  final val registryFromKafkaConfig: ZLayer[Has[KafkaConfig], TamerError, Has[Registry]] = Registry.fromKafkaConfig
  final val kafkaConfigAndRegistryFromEnvironment: Layer[TamerError, Has[KafkaConfig] with Has[Registry]] =
    kafkaConfigFromEnvironment >+> registryFromKafkaConfig
}
