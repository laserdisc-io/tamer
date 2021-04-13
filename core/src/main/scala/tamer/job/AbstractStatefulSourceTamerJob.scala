package tamer.job

import com.sksamuel.avro4s.Codec
import tamer.config.KafkaConfig
import tamer.{SourceConfiguration, TamerError}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{Chunk, Queue, Ref, Schedule, UIO, ZIO}

import java.time.Duration

abstract class AbstractStatefulSourceTamerJob[
    R <: Blocking with Clock with KafkaConfig,
    K: Codec,
    V: Codec,
    S: Codec,
    SS
](genericParameters: SourceConfiguration[K, V, S])
    extends AbstractTamerJob[R, K, V, S](genericParameters) {
  private val sourceStateRef: UIO[Ref[SS]] = Ref.make(createInitialSourceState)

  protected def createInitialSourceState: SS

  protected def createSchedule: Schedule[Any, Any, (Duration, Long)]

  protected def updatedSourceState(currentState: Ref[SS], token: Queue[Unit]): ZIO[R, Throwable, SourceStateChanged]

  override protected final def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] =
    for {
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
      newState <- iteration(sourceState, sourceStateChangedToken)(currentState, q)
    } yield newState

  protected def iteration(
      keysR: Ref[SS],
      keysChangedToken: Queue[Unit]
  )(
      currentState: S,
      q: Queue[Chunk[(K, V)]]
  ): ZIO[R, TamerError, S]
}
