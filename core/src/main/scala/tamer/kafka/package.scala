package tamer

import zio.blocking.Blocking
import zio.clock.Clock
import zio.{Has, ZIO}

package object kafka {
  type Kafka = Has[Kafka.Service]

  final def runLoop: ZIO[Kafka with Blocking with Clock, TamerError, Unit] = ZIO.accessM(_.get.runLoop)
}
