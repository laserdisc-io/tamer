package tamer.rest

import com.sksamuel.avro4s.Codec
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.client3.httpclient.zio.{SttpClient, send}
import sttp.client3.{Identity, Request, RequestT, UriContext, basicRequest}
import sttp.model.Uri
import tamer.config.KafkaConfig
import tamer.job.AbstractTamerJob
import tamer.{AvroCodec, HashableState, TamerError}
import zio.{Chunk, Queue, RIO, Task, UIO, ZEnv, ZIO}

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
      K: Codec,
      V: Codec,
      S: Codec
  ](
      setup: RestConfiguration[R, K, V, S]
  ) = new TamerRestJob[R, K, V, S](setup)

  def withPagination[
      R <: ZEnv with SttpClient with KafkaConfig,
      K <: Product: Codec,
      V <: Product: Codec
  ](
      baseUrl: String,
      pageDecoder: String => RIO[R, DecodedPage[V, Offset]],
      offsetParameterName: String = "page",
      increment: Int = 1
  )(deriveKafkaRecordKey: (Offset, V) => K): TamerRestJob[R, K, V, Offset] = {
    val queryBuilder = new RestQueryBuilder[Offset] {

      /** Used for hashing purposes
        */
      override val queryId: Int = stringHash(baseUrl + offsetParameterName) + increment

      override def query(state: Offset): Request[Either[String, String], Any] = basicRequest.get(uri"$baseUrl".addParam(offsetParameterName, state.offset.toString)).readTimeout(Duration(20, TimeUnit.SECONDS))
    }

    val transitions: RestConfiguration.State[K, V, Offset] =
      RestConfiguration.State(Offset(0))(s => UIO(s.incrementedBy(increment)), deriveKafkaRecordKey)

    new TamerRestJob[R, K, V, Offset](RestConfiguration(queryBuilder = queryBuilder, transitions = transitions, pageDecoder = pageDecoder))
  }
}

class TamerRestJob[
    -R <: ZEnv with SttpClient with KafkaConfig,
    K: Codec,
    V: Codec,
    S: Codec
](
    setup: RestConfiguration[R, K, V, S]
) extends AbstractTamerJob[R, K, V, S](setup.generic) {

  private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.rest")

  private def attemptAuthenticationOnce() = setup.queryBuilder

  private def fetchPageOrRetry(request: RequestT[Identity, Either[String, String], Any]) = send(request).map(_.body).map {
    case Left(error) => auth
    case Right(body) =>
  }

  override protected def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] = {
    val logic: ZIO[R with SttpClient, Throwable, S] = for {
      log <- logTask
      request = setup.queryBuilder.query(currentState)
      _         <- log.info(s"Going to execute request $request with params derived from $currentState") // TODO: revert to debug
      response  <- send(request)
      decodedPage <- {
        response.body match {
          case Left(value) =>
            // assume the cookie expired
            log.error(s"Request $request failed: $value") // <- retry
          case Right(value) => setup.pageDecoder(value)
        }
      }
//        .map(decodedPage =>
//          decodedPage.data.map(value => (setup.transitions.deriveKafkaRecordKey(currentState, value), value))
//            .foreach(c => q.offer(Chunk(c)))
//        )
      nextState <- setup.transitions.getNextState.apply(currentState)
    } yield nextState

    logic.mapError(e => TamerError(e.getLocalizedMessage, e))
  }
}
