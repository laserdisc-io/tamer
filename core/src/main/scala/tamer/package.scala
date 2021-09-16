import zio.{Has, URIO, ZIO, ZLayer}
import zio.blocking.Blocking
import zio.clock.Clock

package object tamer {
  final val kafkaConfig: URIO[Has[KafkaConfig], KafkaConfig]      = ZIO.service
  final val runLoop: ZIO[Has[Tamer] with Clock, TamerError, Unit] = ZIO.accessM(_.get.runLoop)

  implicit final class HashableOps[A](private val _underlying: A) extends AnyVal {
    def hash(implicit A: Hashable[A]): Int = A.hash(_underlying)
  }

  final val kafkaConfigFromEnvironment: ZLayer[Blocking with Clock, TamerError, Has[KafkaConfig]] = KafkaConfig.fromEnvironment
}
