package tamer.rest

import com.sksamuel.avro4s.Codec
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.client3.httpclient.zio.{SttpClient, send}
import tamer.TamerError
import tamer.config.KafkaConfig
import tamer.job.{AbstractTamerJob, SourceStateChanged}
import zio.{Chunk, Queue, Ref, Schedule, Task, ZEnv, ZIO}

object TamerRestJob {
  def apply[
      R <: ZEnv with SttpClient with KafkaConfig,
      K <: Product: Codec,
      V <: Product: Codec,
      S <: Product: Codec
  ](
      setup: RestConfiguration[R, K, V, S]
  ) = new TamerRestJob[R, K, V, S](setup)
}

class TamerRestJob[
    R <: ZEnv with SttpClient with KafkaConfig,
    K <: Product: Codec,
    V <: Product: Codec,
    S <: Product: Codec
](
    setup: RestConfiguration[R, K, V, S]
) extends AbstractTamerJob[R, K, V, S, Unit](setup.generic) {

  private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.rest")

  override protected def createInitialSourceState: Unit = ()

  override protected def createSchedule: Schedule[Any, Any, (java.time.Duration, Long)] =
    Schedule.recurs(0).map(_ => (java.time.Duration.ZERO, 0))

  override protected def updatedSourceState(currentState: Ref[Unit], token: Queue[Unit]): ZIO[R, Throwable, SourceStateChanged] =
    ZIO.succeed(SourceStateChanged(false))

  override protected def iteration(
      keysR: Ref[Unit],
      keysChangedToken: Queue[Unit]
  )(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] = {
    val logic: ZIO[R with SttpClient, Throwable, S] = for {
      log <- logTask
      request = setup.queryBuilder.query(currentState)
      _         <- log.debug(s"Going to execute request $request with params derived from $currentState")
      nextState <- setup.transitions.getNextState.apply(currentState)
      response  <- send(request)
      _ <- {
        response.body match {
          case Left(value) =>
            log.error(s"Request $request failed: $value")
          case Right(value) =>
            value
              .transduce(setup.transducer)
              .map(value => (setup.transitions.deriveKafkaRecordKey(nextState, value), value))
              .foreachChunk(q.offer)
        }
      }
    } yield nextState

    logic.mapError(e => TamerError(e.getLocalizedMessage, e))
  }
}
