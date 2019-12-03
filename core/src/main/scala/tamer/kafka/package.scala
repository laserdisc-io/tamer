package tamer

import tamer.config.KafkaConfig
import zio.{Queue, ZIO}
import zio.blocking.Blocking
import zio.clock.Clock

package object kafka extends Kafka.Service[Kafka] {
  def run[K, V, State, R0, E1 <: TamerError](
      kc: KafkaConfig,
      setup: Setup[K, V, State]
  )(
      f: (State, Queue[(K, V)]) => ZIO[R0, E1, Unit]
  ): ZIO[Kafka with R0 with Blocking with Clock, TamerError, Unit] = ZIO.accessM(_.kafka.run(kc, setup)(f))
}
