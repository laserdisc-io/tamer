import zio.{Has, Layer, URIO, ZIO}
import zio.clock.Clock

package object tamer {
  final type SinkRegistry  = Has[Registry] with Has[TopicName]
  final type StateRegistry = Has[Registry] with Has[TopicName]
  final type TopicName     = String

  final val kafkaConfig: URIO[Has[KafkaConfig], KafkaConfig]      = ZIO.service
  final val runLoop: ZIO[Has[Tamer] with Clock, TamerError, Unit] = ZIO.accessM(_.get.runLoop)

  final type ZSerde[-R, T] = zio.kafka.serde.Serde[R, T]
  final val ZSerde = zio.kafka.serde.Serde

  implicit final class HashableOps[A](private val _underlying: A) extends AnyVal {
    def hash(implicit A: Hashable[A]): Int = A.hash(_underlying)
  }

  final val kafkaConfigFromEnvironment: Layer[TamerError, Has[KafkaConfig]] = KafkaConfig.fromEnvironment
}
