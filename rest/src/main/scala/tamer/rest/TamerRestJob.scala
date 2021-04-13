package tamer.rest

import com.sksamuel.avro4s.Codec
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.capabilities.zio.ZioStreams
import sttp.client3.httpclient.zio.{SttpClient, send}
import sttp.client3.{Identity, RequestT, UriContext, asStreamUnsafe, basicRequest}
import sttp.model.Uri
import tamer.{AvroCodec, HashableState, TamerError}
import tamer.config.KafkaConfig
import tamer.job.AbstractTamerJob
import zio.stream.ZTransducer
import zio.{Chunk, Queue, Task, UIO, ZEnv, ZIO, stream}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.hashing.MurmurHash3.stringHash

object TamerRestJob {
  case class Offset(offset: Int) {
    def incrementedBy(increment: Int): Offset = this.copy(offset = offset + increment)
  }
  object Offset {
    implicit val codec = AvroCodec.codec[Offset]
    implicit val hashableState: HashableState[Offset] = new HashableState[Offset] {

      /** It is required for this hash to be consistent even across executions
        * for the same semantic state. This is in contrast with the built-in
        * `hashCode` method.
        */
      override def stateHash(s: Offset): Int = s.offset
    }
  }

  def apply[
      R <: ZEnv with SttpClient with KafkaConfig,
      K <: Product: Codec,
      V <: Product: Codec,
      S <: Product: Codec
  ](
      setup: RestConfiguration[R, K, V, S]
  ) = new TamerRestJob[R, K, V, S](setup)

  def withPagination[
      R <: ZEnv with SttpClient with KafkaConfig,
      K <: Product: Codec,
      V <: Product: Codec
  ](
      baseUrl: String,
      offsetParameterName: String = "page",
      increment: Int = 1
  )(deriveKafkaRecordKey: (Offset, V) => K): TamerRestJob[R, K, V, Offset] = {
    val queryBuilder = new RestQueryBuilder[Offset] {

      /** Used for hashing purposes
        */
      override val queryId: Int = stringHash(baseUrl + offsetParameterName) + increment

      override def query(state: Offset): Uri = uri"$baseUrl".addParam(offsetParameterName, state.offset.toString)
    }

    val transitions: RestConfiguration.State[K, V, Offset] =
      RestConfiguration.State(Offset(0))(s => UIO(s.incrementedBy(increment)), deriveKafkaRecordKey)

    new TamerRestJob[R, K, V, Offset](RestConfiguration(queryBuilder = queryBuilder, transitions = transitions))
  }
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
      request = basicRequest.get(setup.queryBuilder.query(currentState)).readTimeout(20.seconds)
      _         <- log.debug(s"Going to execute request $request with params derived from $currentState")
      nextState <- setup.transitions.getNextState.apply(currentState)
      response  <- send(request)
      _ <- {
        response.body match {
          case Left(value) =>
            log.error(s"Request $request failed: $value")
          case Right(value) => setup.pageDecoder(value)
            .map(
              _.data.map(value => (setup.transitions.deriveKafkaRecordKey(nextState, value), value))
                .foreach(c => q.offer(Chunk(c)))
            )
        }
      }
    } yield nextState

    logic.mapError(e => TamerError(e.getLocalizedMessage, e))
  }
}
