package tamer.job

import com.sksamuel.avro4s.Codec
import tamer.config.KafkaConfig
import tamer.kafka.Kafka
import tamer.{SourceConfiguration, TamerError}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{Chunk, Queue, Ref, Schedule, UIO, ZIO}

import java.time.Duration


trait TamerJob[R] {
  def fetch(): ZIO[R, TamerError, Unit]
}


abstract class AbstractTamerJob[
  R <: Blocking with Clock with KafkaConfig,
  K <: Product : Codec,
  V <: Product : Codec,
  S <: Product : Codec,
  SS
](genericParameters: SourceConfiguration[K, V, S]) extends TamerJob[R] {
  private val sourceStateRef: UIO[Ref[SS]] = Ref.make(createInitialSourceState)

  protected def createInitialSourceState: SS

  protected def createSchedule: Schedule[Any, Any, (Duration, Long)]

  protected def updatedSourceState(currentState: Ref[SS], token: Queue[Unit]): ZIO[R, Throwable, SourceStateChanged]

  final def fetch(): ZIO[R, TamerError, Unit] = for {
    sourceState <- sourceStateRef
    cappedExponentialBackoff: Schedule[Any, Any, (Duration, Long)] = createSchedule
    sourceStateChangedToken <- Queue.dropping[Unit](requestedCapacity = 1)
    newSourceState = updatedSourceState(sourceState, sourceStateChangedToken)
    _ <- newSourceState
      .scheduleFrom(SourceStateChanged(true))(
        Schedule.once andThen cappedExponentialBackoff.untilInput(_ == SourceStateChanged(true))
      )
      .forever
      .fork
    kafkaLayer = Kafka.live(genericParameters, iteration(sourceState, sourceStateChangedToken))
    _ <- tamer.kafka.runLoop.provideSomeLayer[R](kafkaLayer)
  } yield ()

  protected def iteration(
                           keysR: Ref[SS],
                           keysChangedToken: Queue[Unit]
                         )(
                           currentState: S,
                           q: Queue[Chunk[(K, V)]]
                         ): ZIO[R, TamerError, S]
}
