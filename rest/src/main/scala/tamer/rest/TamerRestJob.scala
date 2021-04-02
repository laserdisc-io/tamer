package tamer.rest

import com.sksamuel.avro4s.Codec
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.client3.httpclient.zio.{SttpClient, send}
import tamer.TamerError
import tamer.config.KafkaConfig
import tamer.job.AbstractTamerJob
import zio.{Chunk, Queue, Task, ZEnv, ZIO}

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
    -R <: ZEnv with SttpClient with KafkaConfig,
    K <: Product: Codec,
    V <: Product: Codec,
    S <: Product: Codec
](
    setup: RestConfiguration[R, K, V, S]
) extends AbstractTamerJob[R, K, V, S](setup.generic) {

  private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.rest")

  override protected def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] = {
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
              .foreachChunk(c => q.offer(c))
        }
      }
    } yield nextState

    logic.mapError(e => TamerError(e.getLocalizedMessage, e))
  }
}
