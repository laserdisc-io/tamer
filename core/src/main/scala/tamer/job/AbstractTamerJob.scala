package tamer.job

import com.sksamuel.avro4s.Codec
import tamer.config.KafkaConfig
import tamer.kafka.Kafka
import tamer.{SourceConfiguration, TamerError}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{Chunk, Queue, ZIO}

abstract class AbstractTamerJob[
    -R <: Blocking with Clock with KafkaConfig,
    K <: Product: Codec,
    V <: Product: Codec,
    S <: Product: Codec
](genericParameters: SourceConfiguration[K, V, S])
    extends TamerJob[R] {

  final def fetch(): ZIO[R, TamerError, Unit] = for {
    kafkaLayer <- ZIO.succeed(Kafka.live(genericParameters, next))
    _          <- tamer.kafka.runLoop.provideSomeLayer[R](kafkaLayer)
  } yield ()

  protected def next(
      currentState: S,
      q: Queue[Chunk[(K, V)]]
  ): ZIO[R, TamerError, S]
}
