package tamer

import zio.blocking.Blocking
import zio.clock.Clock
import zio.{Chunk, Has, Queue, Ref, Schedule, UIO, ZIO}

import java.time.Duration

abstract class AbstractStatefulTamer[R <: Blocking with Clock with Has[KafkaConfig], K, V, S, SS](setup: Setup[K, V, S])
    extends AbstractTamer[R, K, V, S](setup) {
  private val sourceStateRef: UIO[Ref[SS]] = Ref.make(createInitialSourceState)

  protected def createInitialSourceState: SS

  protected def createSchedule: Schedule[Any, Any, (Duration, Long)]

  protected def updatedSourceState(currentState: Ref[SS], token: Queue[Unit]): ZIO[R, Throwable, SourceStateChanged]

  override protected final def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] =
    for {
      sourceState <- sourceStateRef
      cappedExponentialBackoff = createSchedule
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

  protected def iteration(keysR: Ref[SS], keysChangedToken: Queue[Unit])(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S]
}
