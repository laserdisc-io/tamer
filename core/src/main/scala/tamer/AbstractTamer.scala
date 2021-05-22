package tamer

import tamer.kafka.{Kafka, KafkaConfig}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{Chunk, Has, Queue, ZIO}

abstract class AbstractTamer[-R <: Blocking with Clock with Has[KafkaConfig], K, V, S](setup: Setup[K, V, S]) extends Tamer[R] {

  override final val run: ZIO[R, TamerError, Unit] = kafka.runLoop.provideSomeLayer(Kafka.live(setup, next))

  protected def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S]
}
