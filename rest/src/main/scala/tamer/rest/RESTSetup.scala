package tamer
package rest

import java.time.Instant

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.capabilities.{Effect, WebSockets}
import sttp.capabilities.zio.ZioStreams
import sttp.client3.{Request, Response, UriContext, basicRequest}
import sttp.client3.httpclient.zio.{SttpClient, send}
import sttp.model.StatusCode.{Forbidden, NotFound, Unauthorized}
import zio._
import zio.clock.Clock
import zio.duration._

sealed abstract case class RESTSetup[-R, K, V, S: Hashable](
    serdes: Setup.Serdes[K, V, S],
    initialState: S,
    recordKey: (S, V) => K,
    authentication: Option[Authentication[R]],
    queryFor: S => Request[Either[String, String], ZioStreams with Effect[Task] with WebSockets],
    pageDecoder: String => RIO[R, DecodedPage[V, S]],
    filterPage: (DecodedPage[V, S], S) => List[V],
    stateFold: (DecodedPage[V, S], S) => URIO[R, S],
    retrySchedule: Option[
      SttpRequest => Schedule[Any, FallibleResponse, FallibleResponse]
    ] = None
) extends Setup[R with SttpClient with Clock with Has[EphemeralSecretCache], K, V, S] {

  private[this] final val query            = queryFor(initialState).show(includeHeaders = false, includeBody = false)
  private[this] final val queryHash        = query.hash
  private[this] final val initialStateHash = initialState.hash

  override final val stateKey = queryHash + initialStateHash
  override final val repr =
    s"""query:              $query
       |query hash:         $queryHash
       |initial state:      $initialState
       |initial state hash: ${initialState.hash}
       |state key:          $stateKey
       |""".stripMargin

  protected final val logTask = log4sFromName.provide("tamer.rest")

  protected def fetchAndDecodePage(
      request: SttpRequest,
      secretCacheRef: EphemeralSecretCache,
      log: LogWriter[Task]
  ): RIO[R with SttpClient with Clock, DecodedPage[V, S]] = {
    def sendRequestRepeatMaybe(request: SttpRequest) =
      retrySchedule.fold[ZIO[SttpClient with Clock, Throwable, Response[Either[String, String]]]](send(request))(schedule =>
        send(request).either.repeat(schedule(request)).absolve
      )

    def authenticateAndSendRequest(auth: Authentication[R]): RIO[R with SttpClient with Clock, Response[Either[String, String]]] = {

      def authenticateAndSendHelper(maybeToken: Option[String]): RIO[SttpClient with Clock, Response[Either[String, String]]] = {
        val authenticatedRequest = auth.addAuthentication(request, maybeToken)
        log.debug(s"going to execute request $authenticatedRequest") *> sendRequestRepeatMaybe(authenticatedRequest)
      }

      for {
        _             <- secretCacheRef.get.flatMap(_.fold(auth.setSecret(secretCacheRef))(_ => UIO.unit))
        maybeToken    <- secretCacheRef.get
        firstResponse <- authenticateAndSendHelper(maybeToken)
        response <- firstResponse.code match {
          case Forbidden | Unauthorized | NotFound =>
            log.warn("authentication failed, assuming credentials are expired, will retry, possibly refreshing them...") *>
              log.debug(s"response was: ${firstResponse.body}") *>
              auth.refreshSecret(secretCacheRef) *>
              secretCacheRef.get.flatMap(authenticateAndSendHelper)
          case _ => UIO(firstResponse) <* log.debug("authentication succeeded: proceeding as normal")
        }
      } yield response
    }

    for {
      response <- authentication.map(authenticateAndSendRequest).getOrElse(sendRequestRepeatMaybe(request))
      decodedPage <- response.body match {
        case Left(error) =>
          // assume the cookie/auth expired
          log.error(s"request failed with error '$error'") *>
            secretCacheRef.set(None) *> // clear cache
            RIO.fail(TamerError("request failed, giving up."))
        case Right(body) =>
          pageDecoder(body)
      }
    } yield decodedPage
  }

  // TODO: it seems the common pattern for the specializations of this function is to schedule
  // `fetchAndDecode` page depending on the current state. Either the execution is delayed
  // (for example when we have to repeat all the queries after resetting the offset) or repeated
  // (for example when we are waiting new pages to appear). This has to be combined with an
  // intuitive filtering of the page result and automatic type inference of the state for
  // the page decoder helper functions.
  override def iteration(
      currentState: S,
      queue: Enqueue[NonEmptyChunk[(K, V)]]
  ): RIO[R with SttpClient with Clock with Has[EphemeralSecretCache], S] = for {
    log         <- logTask
    tokenCache  <- ZIO.service[EphemeralSecretCache]
    decodedPage <- fetchAndDecodePage(queryFor(currentState), tokenCache, log)
    chunk = Chunk.fromIterable(filterPage(decodedPage, currentState).map(value => recordKey(currentState, value) -> value))
    _         <- NonEmptyChunk.fromChunk(chunk).map(queue.offer).getOrElse(UIO.unit)
    nextState <- stateFold(decodedPage, currentState)
  } yield nextState
}

object RESTSetup {
  def apply[R, K: Codec, V: Codec, S: Codec: Hashable](initialState: S)(
      query: S => Request[Either[String, String], ZioStreams with Effect[Task] with WebSockets],
      pageDecoder: String => RIO[R, DecodedPage[V, S]],
      recordKey: (S, V) => K,
      stateFold: (DecodedPage[V, S], S) => URIO[R, S],
      authentication: Option[Authentication[R]] = None,
      filterPage: (DecodedPage[V, S], S) => List[V] = (dp: DecodedPage[V, S], _: S) => dp.data,
      retrySchedule: Option[
        SttpRequest => Schedule[Any, FallibleResponse, FallibleResponse]
      ] = None
  )(
      implicit ev: Codec[Tamer.StateKey]
  ): RESTSetup[R, K, V, S] = new RESTSetup(
    Setup.mkSerdes[K, V, S],
    initialState,
    recordKey,
    authentication,
    query,
    pageDecoder,
    filterPage,
    stateFold,
    retrySchedule
  ) {}

  def paginated[R, K: Codec, V: Codec](
      baseUrl: String,
      pageDecoder: String => RIO[R, DecodedPage[V, Offset]],
      authentication: Option[Authentication[R]] = None,
      retrySchedule: Option[
        SttpRequest => Schedule[Any, FallibleResponse, FallibleResponse]
      ] = None
  )(
      recordKey: (Offset, V) => K,
      offsetParameterName: String = "page",
      increment: Int = 1,
      fixedPageElementCount: Option[Int] = None,
      readRequestTimeout: Duration = 30.seconds,
      initialOffset: Offset = Offset(0, 0)
  )(
      implicit ev0: Codec[Tamer.StateKey],
      ev1: Codec[Offset]
  ): RESTSetup[R with Clock, K, V, Offset] = {
    def queryFor(state: Offset) =
      basicRequest.get(uri"$baseUrl".addParam(offsetParameterName, state.offset.toString)).readTimeout(readRequestTimeout.asScala)

    def nextPageOrNextIndexIfPageNotComplete(decodedPage: DecodedPage[V, Offset], offset: Offset): URIO[R with Clock, Offset] = {
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

    new RESTSetup(
      Setup.mkSerdes[K, V, Offset],
      initialOffset,
      recordKey,
      authentication,
      queryFor,
      pageDecoder,
      filterPage,
      nextPageOrNextIndexIfPageNotComplete,
      retrySchedule
    ) {
      override def iteration(
          currentState: Offset,
          queue: Enqueue[NonEmptyChunk[(K, V)]]
      ): RIO[R with Clock with SttpClient with Has[EphemeralSecretCache], Offset] = for {
        log         <- logTask
        tokenCache  <- ZIO.service[EphemeralSecretCache]
        decodedPage <- fetchWaitingNewEntries(currentState, log, tokenCache)
        chunk = Chunk.fromIterable(filterPage(decodedPage, currentState).map(value => recordKey(currentState, value) -> value))
        _         <- NonEmptyChunk.fromChunk(chunk).map(queue.offer).getOrElse(UIO.unit)
        nextState <- stateFold(decodedPage, currentState)
      } yield nextState

      private final def fetchWaitingNewEntries(
          currentState: Offset,
          log: LogWriter[Task],
          tokenCache: EphemeralSecretCache
      ): RIO[R with Clock with SttpClient, DecodedPage[V, Offset]] =
        fetchAndDecode(currentState, tokenCache, log).repeat(
          Schedule.exponential(500.millis) *> Schedule.recurWhile(_.data.isEmpty) // FIXME this could never return and leaves tamer hanging
        )

      private final def fetchAndDecode(
          currentState: Offset,
          tokenCache: EphemeralSecretCache,
          log: LogWriter[Task]
      ): RIO[R with Clock with SttpClient, DecodedPage[V, Offset]] =
        for {
          decodedPage <- fetchAndDecodePage(queryFor(currentState), tokenCache, log)
          dataFromIndex = decodedPage.data.drop(currentState.nextIndex)
          pageLength    = decodedPage.data.length
          _ <- log.debug(s"retrieved ${dataFromIndex.length} new datapoints. ($pageLength total for page ${currentState.offset})")
        } yield decodedPage
    }
  }

  def periodicallyPaginated[R, K: Codec, V: Codec](
      baseUrl: String,
      pageDecoder: String => RIO[R, DecodedPage[V, PeriodicOffset]],
      periodStart: Instant,
      authentication: Option[Authentication[R]] = None,
      offsetParameterName: String = "page",
      increment: Int = 1,
      minPeriod: Duration = 5.minutes,
      maxPeriod: Duration = 1.hour,
      readRequestTimeout: Duration = 30.seconds,
      startingOffset: Int = 0,
      filterPage: (DecodedPage[V, PeriodicOffset], PeriodicOffset) => List[V] = (dp: DecodedPage[V, PeriodicOffset], _: PeriodicOffset) => dp.data,
      retrySchedule: Option[
        SttpRequest => Schedule[Any, FallibleResponse, FallibleResponse]
      ] = None
  )(recordKey: (PeriodicOffset, V) => K)(
      implicit ev0: Codec[Tamer.StateKey],
      ev1: Codec[PeriodicOffset]
  ): RESTSetup[R with Clock, K, V, PeriodicOffset] = {
    def queryFor(state: PeriodicOffset) =
      basicRequest.get(uri"$baseUrl".addParam(offsetParameterName, state.offset.toString)).readTimeout(readRequestTimeout.asScala)

    def getNextState(decodedPage: DecodedPage[V, PeriodicOffset], periodicOffset: PeriodicOffset): URIO[R with Clock, PeriodicOffset] =
      decodedPage.nextState.map(UIO(_)).getOrElse {
        for {
          now <- zio.clock.instant
          newOffset <-
            if (
              now.isAfter(periodicOffset.periodStart.plus(maxPeriod)) ||
              decodedPage.data.isEmpty && now.isAfter(periodicOffset.periodStart.plus(minPeriod))
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

    new RESTSetup(
      Setup.mkSerdes[K, V, PeriodicOffset],
      PeriodicOffset(startingOffset, periodStart),
      recordKey,
      authentication,
      queryFor,
      pageDecoder,
      filterPage,
      getNextState,
      retrySchedule
    ) {
      override def iteration(
          currentState: PeriodicOffset,
          queue: Enqueue[NonEmptyChunk[(K, V)]]
      ): RIO[R with Clock with SttpClient with Has[EphemeralSecretCache], PeriodicOffset] = for {
        log        <- logTask
        tokenCache <- ZIO.service[EphemeralSecretCache]
        now        <- clock.instant
        delayUntilNextPeriod <-
          if (currentState.periodStart.isBefore(now))
            UIO(Duration.Zero)
          else
            UIO(Duration.fromInterval(now, currentState.periodStart)).tap(delayUntilNextPeriod =>
              log.info(s"$baseUrl is going to sleep for ${delayUntilNextPeriod.render}")
            )
        decodedPage <- fetchAndDecodePage(queryFor(currentState), tokenCache, log).delay(delayUntilNextPeriod)
        chunk = Chunk.fromIterable(filterPage(decodedPage, currentState).map(value => recordKey(currentState, value) -> value))
        _         <- NonEmptyChunk.fromChunk(chunk).map(queue.offer).getOrElse(UIO.unit)
        nextState <- getNextState(decodedPage, currentState)
      } yield nextState
    }
  }
}
