package tamer
package rest

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.client3.httpclient.zio.{SttpClient, send}
import sttp.client3.{Request, Response, UriContext, basicRequest}
import sttp.model.StatusCode.{Forbidden, NotFound, Unauthorized}
import tamer.rest.LocalSecretCache.LocalSecretCache
import zio.clock.Clock
import zio.duration.{durationInt, Duration => ZDuration}
import zio.{Chunk, Has, Queue, RIO, Ref, Schedule, Task, UIO, ULayer, URIO, ZEnv, ZIO, ZLayer, clock}

import java.time.Instant
import scala.annotation.{nowarn, unused}
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

object RESTTamer {
  case class Offset(offset: Int, nextIndex: Int) {
    def incrementedBy(increment: Int): Offset = this.copy(offset = offset + increment, nextIndex = 0)
    def nextIndex(index: Int): Offset         = this.copy(nextIndex = index)
  }
  object Offset {
    implicit val hashable: Hashable[Offset] = s => s.offset * s.nextIndex
  }

  case class PeriodicOffset(offset: Int, periodStart: Instant) {
    def incrementedBy(increment: Int): PeriodicOffset = this.copy(offset = offset + increment)
  }
  object PeriodicOffset {
    implicit val hashable: Hashable[PeriodicOffset] = s => s.offset * (s.periodStart.getEpochSecond + s.periodStart.getNano.longValue).toInt
  }

  def apply[R <: ZEnv with SttpClient with Has[KafkaConfig] with LocalSecretCache, K, V, S](setup: RESTSetup[R, K, V, S]) =
    new RESTTamer[R, K, V, S](setup)

  def withPagination[
      R <: ZEnv with SttpClient with Has[KafkaConfig] with LocalSecretCache,
      K: Codec,
      V: Codec
  ](
      baseUrl: String,
      pageDecoder: String => RIO[R, DecodedPage[V, Offset]],
      offsetParameterName: String = "page",
      increment: Int = 1,
      fixedPageElementCount: Option[Int] = None,
      authenticationMethod: Option[Authentication[R]] = None,
      readRequestTimeout: zio.duration.Duration = 30.seconds,
      initialOffset: Offset = Offset(0, 0)
  )(deriveKafkaRecordKey: (Offset, V) => K): RESTTamer[R, K, V, Offset] = {
    val queryBuilder: QueryBuilder[R, Offset] = new QueryBuilder[R, Offset] {

      /** Used for hashing purposes
        */
      override val queryId: Int = stringHash(baseUrl + offsetParameterName) + increment

      override def query(state: Offset): Request[Either[String, String], Any] =
        basicRequest
          .get(uri"$baseUrl".addParam(offsetParameterName, state.offset.toString))
          .readTimeout(Duration.fromNanos(readRequestTimeout.toNanos))

      override val authentication: Option[Authentication[R]] = authenticationMethod
    }

    def nextPageOrNextIndexIfPageNotComplete(decodedPage: DecodedPage[V, Offset], offset: Offset): UIO[Offset] = {
      val fetchedElementCount = decodedPage.data.length
      val nextElementIndex    = decodedPage.data.length
      lazy val defaultNextState = fixedPageElementCount match {
        case Some(expectedElemCount) if fetchedElementCount <= expectedElemCount - 1 => UIO(offset.nextIndex(nextElementIndex))
        case _                                                                       => UIO(offset.incrementedBy(increment))
      }
      decodedPage.nextState.map(UIO(_)).getOrElse(defaultNextState)
    }

    def filterPage(decodedPage: DecodedPage[V, Offset], offset: Offset): List[V] = {
      val previousElementCount = offset.nextIndex
      decodedPage.data.drop(previousElementCount)
    }

    @nowarn
    val transitions: RESTSetup.State[R, K, V, Offset] =
      new RESTSetup.State(initialOffset)(deriveKafkaRecordKey)(nextPageOrNextIndexIfPageNotComplete, filterPage)

    val setup = new RESTSetup(queryBuilder = queryBuilder, pageDecoder = pageDecoder)(transitions = transitions)

    new RESTTamer[R, K, V, Offset](setup) {
      override protected def next(currentState: Offset, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, Offset] = {
        val logic: RIO[R with SttpClient with LocalSecretCache, Offset] = for {
          log         <- logTask
          tokenCache  <- ZIO.service[Ref[Option[String]]]
          decodedPage <- fetchWaitingNewEntries(currentState, log, tokenCache)
          chunk = Chunk.fromIterable(
            setup.transitions.filterPage(decodedPage, currentState).map(value => (setup.transitions.deriveKafkaRecordKey(currentState, value), value))
          )
          _         <- q.offer(chunk)
          nextState <- setup.transitions.getNextState(decodedPage, currentState)
        } yield nextState

        logic.mapError(e => TamerError(e.getLocalizedMessage, e))
      }

      private final def fetchWaitingNewEntries(
          currentState: Offset,
          log: LogWriter[Task],
          tokenCache: Ref[Option[String]]
      ): RIO[R with Clock, DecodedPage[V, Offset]] =
        fetchAndDecode(currentState, tokenCache, log).repeat(
          Schedule.exponential(500.millis) *> Schedule.recurWhile(_.data.isEmpty)
        )

      private final def fetchAndDecode(currentState: Offset, tokenCache: Ref[Option[String]], log: LogWriter[Task]): RIO[R, DecodedPage[V, Offset]] =
        for {
          decodedPage <- fetchAndDecodePage(setup.queryBuilder.query(currentState), tokenCache, log)
          dataFromIndex = decodedPage.data.drop(currentState.nextIndex)
          pageLength    = decodedPage.data.length
          _ <- log.debug(s"Fetch and Decode retrieved ${dataFromIndex.length} new datapoints. ($pageLength total for page ${currentState.offset})")
        } yield decodedPage
    }
  }

  def withPaginationPeriodic[
      R <: ZEnv with SttpClient with Has[KafkaConfig] with LocalSecretCache,
      K: Codec,
      V: Codec
  ](
      baseUrl: String,
      pageDecoder: String => RIO[R, DecodedPage[V, PeriodicOffset]],
      periodStart: Instant,
      offsetParameterName: String = "page",
      increment: Int = 1,
      authenticationMethod: Option[Authentication[R]] = None,
      minPeriod: zio.duration.Duration = 5.minutes,
      maxPeriod: zio.duration.Duration = 1.hour,
      readRequestTimeout: zio.duration.Duration = 30.seconds,
      startingOffset: Int = 0
  )(deriveKafkaRecordKey: (PeriodicOffset, V) => K): RESTTamer[R, K, V, PeriodicOffset] = {
    val queryBuilder: QueryBuilder[R with Clock, PeriodicOffset] = new QueryBuilder[R, PeriodicOffset] {

      /** Used for hashing purposes
        */
      override val queryId: Int = stringHash(baseUrl + offsetParameterName) + increment

      override def query(state: PeriodicOffset): Request[Either[String, String], Any] =
        basicRequest
          .get(uri"$baseUrl".addParam(offsetParameterName, state.offset.toString))
          .readTimeout(Duration.fromNanos(readRequestTimeout.toNanos))

      override val authentication: Option[Authentication[R]] = authenticationMethod
    }

    def getNextState(decodedPage: DecodedPage[V, PeriodicOffset], periodicOffset: PeriodicOffset): URIO[R with Clock, PeriodicOffset] =
      decodedPage.nextState.map(UIO(_)).getOrElse {
        for {
          now <- zio.clock.instant
          newOffset <-
            if (
              now.isAfter(periodicOffset.periodStart.plus(maxPeriod)) ||
              (decodedPage.data.isEmpty && now.isAfter(periodicOffset.periodStart.plus(minPeriod)))
            ) {
              UIO(PeriodicOffset(offset = startingOffset, periodStart = now))
            } else if (decodedPage.data.isEmpty) {
              val nextPeriodStart: Instant = periodicOffset.periodStart.plus(minPeriod)
              UIO(PeriodicOffset(offset = startingOffset, periodStart = nextPeriodStart))
            } else {
              UIO(periodicOffset.incrementedBy(increment))
            }
        } yield newOffset
      }

    val transitions =
      new RESTSetup.State[R with Clock, K, V, PeriodicOffset](PeriodicOffset(startingOffset, periodStart))(deriveKafkaRecordKey)(getNextState)

    val setup = new RESTSetup(queryBuilder = queryBuilder, pageDecoder = pageDecoder)(transitions = transitions)

    new RESTTamer[R with Clock, K, V, PeriodicOffset](setup) {
      override protected def next(currentState: PeriodicOffset, q: Queue[Chunk[(K, V)]]): ZIO[R with Clock, TamerError, PeriodicOffset] = {
        val logic: RIO[R with SttpClient with LocalSecretCache with Clock, PeriodicOffset] = for {
          log        <- logTask
          tokenCache <- ZIO.service[Ref[Option[String]]]
          now        <- clock.instant
          delayUntilNextPeriod <-
            if (currentState.periodStart.isBefore(now))
              UIO(ZDuration.Zero)
            else
              UIO(ZDuration.fromInterval(now, currentState.periodStart)).tap(delayUntilNextPeriod =>
                log.info(s"Time until the next period: ${delayUntilNextPeriod.toString}")
              )
          decodedPage <- fetchAndDecodePage(setup.queryBuilder.query(currentState), tokenCache, log).delay(delayUntilNextPeriod)
          chunk = Chunk.fromIterable(
            setup.transitions.filterPage(decodedPage, currentState).map(value => (setup.transitions.deriveKafkaRecordKey(currentState, value), value))
          )
          _         <- q.offer(chunk)
          nextState <- setup.transitions.getNextState(decodedPage, currentState)
        } yield nextState

        logic.mapError(e => TamerError(e.getLocalizedMessage, e))
      }
    }
  }
}

class RESTTamer[-R <: ZEnv with SttpClient with Has[KafkaConfig] with LocalSecretCache, K, V, S](setup: RESTSetup[R, K, V, S])
    extends AbstractTamer[R, K, V, S](setup.generic) {

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

  // TODO: it seems the common pattern for the specializations of this function is to schedule
  // `fetchAndDecode` page depending on the current state. Either the execution is delayed
  // (for example when we have to repeat all the queries after resetting the offset) or repeated
  // (for example when we are waiting new pages to appear). This has to be combined with an
  // intuitive filtering of the page result and automatic type inference of the state for
  // the page decoder helper functions.
  override protected def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] = {
    val logic: RIO[R with SttpClient with LocalSecretCache, S] = for {
      log         <- logTask
      tokenCache  <- ZIO.service[Ref[Option[String]]]
      decodedPage <- fetchAndDecodePage(setup.queryBuilder.query(currentState), tokenCache, log)
      chunk = Chunk.fromIterable(
        setup.transitions.filterPage(decodedPage, currentState).map(value => (setup.transitions.deriveKafkaRecordKey(currentState, value), value))
      )
      _         <- q.offer(chunk)
      nextState <- setup.transitions.getNextState(decodedPage, currentState)
    } yield nextState

    logic.mapError(e => TamerError(e.getLocalizedMessage, e))
  }
}
