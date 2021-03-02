package tamer.job

import com.sksamuel.avro4s.Codec
import tamer.{SourceConfiguration, TamerError}
import zio.{Queue, Ref, Schedule, UIO, ZIO}

import java.time.Duration

abstract class AbstractTamerJob[
  K <: Product : Codec,
  V <: Product : Codec,
  S <: Product : Codec,
  SourceState,
  R <: tamer.kafka.Kafka with zio.blocking.Blocking with zio.clock.Clock
](generic: SourceConfiguration[K, V, S]) {
  protected type SourceStateRef = Ref[SourceState]

  private val initialSourceStateRef: UIO[SourceStateRef] = Ref.make(createInitialSourceState())

  protected def createInitialSourceState(): SourceState


  final def fetch(): ZIO[R, TamerError, Unit] =
    for {
      keysR <- initialSourceStateRef
      keysChangedToken <- Queue.dropping[Unit](requestedCapacity = 1)
      updateListOfKeysM = updateSourceState(
        keysR,
        keysChangedToken
      )
      updateKeysFiber <- updateListOfKeysM
        .scheduleFrom(SourceStateChanged(true))(
          Schedule.once andThen cappedExponentialBackoff.untilInput(_ == SourceStateChanged(true))
        )
        .forever
        .fork
      _ <- tamer.kafka.runLoop(generic)(iteration(keysR, keysChangedToken)).ensuring(updateKeysFiber.interrupt)
    } yield ()

  protected def cappedExponentialBackoff: Schedule[Any, Any, (Duration, Long)]

  protected def updateSourceState(
                                   currentSourceState: SourceStateRef,
                                   sourceChangedToken: Queue[Unit]
                                ): ZIO[R, Throwable, SourceStateChanged]


  protected def iteration(
                           currentSourceState: SourceStateRef,
                           sourceChangedToken: Queue[Unit]
                         )(
                           currentState: S,
                           q: Queue[(K, V)]
                         ): ZIO[R, TamerError, S]
}
