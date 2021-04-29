package tamer.rest

import com.sksamuel.avro4s.Codec
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.client3.httpclient.zio.{SttpClient, send}
import sttp.client3.{Request, Response, UriContext, basicRequest}
import sttp.model.StatusCode.{Forbidden, NotFound, Unauthorized}
import tamer.config.KafkaConfig
import tamer.job.AbstractTamerJob
import tamer.rest.LocalSecretCache.LocalSecretCache
import tamer.{AvroCodec, HashableState, TamerError}
import zio.duration.durationInt
import zio.{Chunk, Has, Queue, RIO, Ref, Schedule, Task, UIO, ULayer, ZEnv, ZIO, ZLayer}

import java.util.concurrent.TimeUnit
import scala.annotation.unused
import scala.concurrent.duration.Duration
import scala.util.hashing.MurmurHash3.stringHash

trait Authentication[-R] {
  def addAuthentication(request: SttpRequest, supplementalSecret: Option[String]): SttpRequest

  def setSecret(@unused secretRef: Ref[Option[String]]): ZIO[R, TamerError, Unit] = UIO(())
  // TODO: could return the value to set instead of unit

  def refreshSecret(secretRef: Ref[Option[String]]): ZIO[R, TamerError, Unit] = setSecret(secretRef)
  // TODO: widen error since this is user space
}

object Authentication {
  def basic[R](username: String, password: String): Some[Authentication[R]] =
    Some((request: SttpRequest, _: Option[String]) => request.auth.basic(username, password))
}

object LocalSecretCache {
  type LocalSecretCache = Has[Ref[Option[String]]]
  val localSecretCacheM: UIO[Ref[Option[String]]] = Ref.make[Option[String]](None)
  val live: ULayer[LocalSecretCache]              = ZLayer.fromEffect(localSecretCacheM)
}

object TamerRestJob {
  case class Offset(offset: Int, nextIndex: Int) {
    def incrementedBy(increment: Int): Offset = this.copy(offset = offset + increment, nextIndex = 0)
    def nextIndex(index: Int): Offset         = this.copy(nextIndex = index)
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
      R <: ZEnv with SttpClient with KafkaConfig with LocalSecretCache,
      K: Codec,
      V: Codec,
      S: Codec
  ](
      setup: RestConfiguration[R, K, V, S]
  ) = new TamerRestJob[R, K, V, S](setup)

  def withPagination[
      R <: ZEnv with SttpClient with KafkaConfig with LocalSecretCache,
      K: Codec,
      V: Codec
  ](
      baseUrl: String,
      pageDecoder: String => RIO[R, DecodedPage[V, Offset]],
      offsetParameterName: String = "page",
      increment: Int = 1,
      fixedPageElementCount: Option[Int] = None,
      authenticationMethod: Option[Authentication[R]] = None
  )(deriveKafkaRecordKey: (Offset, V) => K): TamerRestJob[R, K, V, Offset] = {
    val queryBuilder: RestQueryBuilder[R, Offset] = new RestQueryBuilder[R, Offset] {

      /** Used for hashing purposes
        */
      override val queryId: Int = stringHash(baseUrl + offsetParameterName) + increment

      override def query(state: Offset): Request[Either[String, String], Any] =
        basicRequest.get(uri"$baseUrl".addParam(offsetParameterName, state.offset.toString)).readTimeout(Duration(20, TimeUnit.SECONDS))

      override val authentication: Option[Authentication[R]] = authenticationMethod
    }

    def getNextState(offset: Offset): UIO[Offset] = fixedPageElementCount match {
      case Some(expectedElemCount) if offset.nextIndex <= expectedElemCount - 1 => UIO(offset)
      case _                                                                    => UIO(offset.incrementedBy(increment))
    }

    val transitions: RestConfiguration.State[K, V, Offset] =
      new RestConfiguration.State(Offset(0, 0))(getNextState, deriveKafkaRecordKey)

    val setup = new RestConfiguration(queryBuilder = queryBuilder, pageDecoder = pageDecoder)(transitions = transitions)
    new TamerRestJob[R, K, V, Offset](setup) {
      private final def fetchAndDecode(currentState: Offset, tokenCache: Ref[Option[String]], log: LogWriter[Task]): RIO[R, (List[V], Int)] = for {
        decodedPage <- fetchAndDecodePage(setup.queryBuilder.query(currentState), tokenCache, log)
        dataFromIndex = decodedPage.data.drop(currentState.nextIndex)
        pageLength    = decodedPage.data.length
        _ <- log.debug(s"Fetch and Decode retrieved ${dataFromIndex.length} new datapoints. ($pageLength total for page ${currentState.offset})")
      } yield (dataFromIndex, pageLength)

      override protected def next(currentState: Offset, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, Offset] = {
        val logic: RIO[R with SttpClient with LocalSecretCache, Offset] = for {
          log                         <- this.logTask
          tokenCache                  <- ZIO.service[Ref[Option[String]]]
          (dataFromIndex, pageLength) <- fetchAndDecode(currentState, tokenCache, log).repeat(Schedule.exponential(500.millis) *> Schedule.recurWhile { case (data, _) => data.isEmpty })
          _                           <- ZIO.foreach_(dataFromIndex.map(value => (setup.transitions.deriveKafkaRecordKey(currentState, value), value)))(c => q.offer(Chunk(c)))
          nextState                   <- setup.transitions.getNextState.apply(currentState.nextIndex(pageLength))
        } yield nextState

        logic.mapError(e => TamerError(e.getLocalizedMessage, e))
      }
    }
  }
}

class TamerRestJob[
    -R <: ZEnv with SttpClient with KafkaConfig with LocalSecretCache,
    K: Codec,
    V: Codec,
    S: Codec
](
    setup: RestConfiguration[R, K, V, S]
) extends AbstractTamerJob[R, K, V, S](setup.generic) {
  protected[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.rest")

  protected def fetchAndDecodePage(request: SttpRequest, tokenCacheRef: Ref[Option[String]], log: LogWriter[Task]): RIO[R, DecodedPage[V, S]] = {
    def authenticateAndSendRequest(
        requestToTransform: SttpRequest,
        tokenCacheRef: Ref[Option[String]],
        log: LogWriter[Task]
    ): RIO[R, Response[Either[String, String]]] = {
      val auth = setup.queryBuilder.authentication.get // FIXME: unsafe

      def authenticateAndSendHelper(maybeToken: Option[String]): ZIO[SttpClient, Throwable, Response[Either[String, String]]] = {
        val authenticatedRequest = auth.addAuthentication(requestToTransform, maybeToken)
        log.debug(s"Going to execute request $authenticatedRequest") *> send(authenticatedRequest)
      }

      for {
        maybeToken <- tokenCacheRef.get
        _          <- log.info(s"Currently the token cache contains: $maybeToken")
        _ <- maybeToken match {
          case Some(_) => UIO.unit // secret is already present do nothing
          case None    => auth.setSecret(tokenCacheRef)
        }
        firstResponse <- authenticateAndSendHelper(maybeToken)
        response <- firstResponse.code match {
          case Forbidden | Unauthorized | NotFound =>
            log.warn("Authentication failed, assuming credentials are expired, will retry, possibly refreshing them...") *>
              log.debug(s"Response was: ${firstResponse.body}") *>
              auth.refreshSecret(tokenCacheRef) *>
              tokenCacheRef.get.flatMap(authenticateAndSendHelper)
          case _ => UIO(firstResponse) <* log.debug("Authentication succeeded: proceeding as normal")
        }
      } yield response
    }

    for {
      response <- setup.queryBuilder.authentication match {
        case Some(_) => authenticateAndSendRequest(request, tokenCacheRef, log)
        case None    => send(request)
      }
      decodedPage <- response.body match {
        case Left(error) =>
          // assume the cookie/auth expired
          log.error(s"""Request failed with error "$error"""") *>
            tokenCacheRef.set(None) *> // clear cache
            RIO.fail(TamerError("Request failed, giving up."))
        case Right(body) =>
          setup.pageDecoder(body)
      }
    } yield decodedPage
  }

  override protected def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] = {
    val logic: RIO[R with SttpClient with LocalSecretCache, S] = for {
      log         <- logTask
      tokenCache  <- ZIO.service[Ref[Option[String]]]
      decodedPage <- fetchAndDecodePage(setup.queryBuilder.query(currentState), tokenCache, log)
      _           <- ZIO.foreach_(decodedPage.data.map(value => (setup.transitions.deriveKafkaRecordKey(currentState, value), value)))(c => q.offer(Chunk(c)))
      nextState   <- setup.transitions.getNextState.apply(decodedPage.nextState.getOrElse(currentState))
    } yield nextState

    logic.mapError(e => TamerError(e.getLocalizedMessage, e))
  }
}
