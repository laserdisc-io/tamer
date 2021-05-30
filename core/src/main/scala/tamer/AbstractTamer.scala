package tamer

import zio.blocking.Blocking
import zio.clock.Clock
import zio.{Chunk, Has, Queue, ZIO}

abstract class AbstractTamer[-R <: Blocking with Clock with Has[KafkaConfig], K, V, S](setup: Setup[K, V, S]) {
  final val run: ZIO[R, TamerError, Unit] = runLoop.provideSomeLayer(Tamer.live(setup, next))

  protected def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S]
}
