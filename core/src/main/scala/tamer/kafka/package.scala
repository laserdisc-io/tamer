package tamer

import zio.{Chunk, Has, Queue, ZIO}
import zio.blocking.Blocking
import zio.clock.Clock

package object kafka {
  type Kafka = Has[Kafka.Service]

  final def runLoop[K, V, State, R](setup: SourceConfiguration[K, V, State])(
      f: (State, Queue[(K, V)]) => ZIO[R, TamerError, State]
  ): ZIO[Kafka with R with Blocking with Clock, TamerError, Unit] = ZIO.accessM(_.get.runLoop(setup)(f))
}
