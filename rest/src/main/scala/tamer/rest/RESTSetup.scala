package tamer
package rest

import java.time.Instant

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.capabilities.{Effect, WebSockets}
import sttp.capabilities.zio.ZioStreams
import sttp.client4.{Request, Response, UriContext, basicRequest}
import sttp.client4.httpclient.zio.{SttpClient, send}
import sttp.model.StatusCode.{Forbidden, NotFound, Unauthorized}
import zio._

sealed abstract case class RESTSetup[-R, K: Tag, V: Tag, SV: Tag: Hashable](
    initialState: SV,
    recordKey: (SV, V) => K,
    authentication: Option[Authentication[R]],
    queryFor: SV => SttpRequest,
    pageDecoder: String => RIO[R, DecodedPage[V, SV]],
    filterPage: (DecodedPage[V, SV], SV) => List[V],
    stateFold: (DecodedPage[V, SV], SV) => URIO[R, SV],
    retrySchedule: Option[
      SttpRequest => Schedule[Any, FallibleResponse, FallibleResponse]
    ] = None
)(
    implicit ev: SerdesProvider[K, V, SV]
) extends Setup[R with SttpClient with EphemeralSecretCache, K, V, SV] {

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

  protected final val logTask = log4sFromName.provideEnvironment(ZEnvironment("tamer.rest"))

  protected def fetchAndDecodePage(
      request: SttpRequest,
      secretCacheRef: EphemeralSecretCache,
      log: LogWriter[Task]
  ): RIO[R with SttpClient, DecodedPage[V, SV]] = {
    def sendRequestRepeatMaybe(request: SttpRequest) =
      retrySchedule.fold[RIO[SttpClient, Response[Either[String, String]]]](send(request))(schedule =>
        send(request).either.repeat(schedule(request)).absolve
      )

    def authenticateAndSendRequest(auth: Authentication[R]): RIO[R with SttpClient, Response[Either[String, String]]] = {

      def authenticateAndSendHelper(maybeToken: Option[String]): RIO[SttpClient, Response[Either[String, String]]] = {
        val authenticatedRequest = auth.addAuthentication(request, maybeToken)
        log.debug(s"going to execute request $authenticatedRequest") *> sendRequestRepeatMaybe(authenticatedRequest)
      }

      for {
        _             <- secretCacheRef.get.flatMap(_.fold(auth.setSecret(secretCacheRef))(_ => ZIO.unit))
        maybeToken    <- secretCacheRef.get
        firstResponse <- authenticateAndSendHelper(maybeToken)
        response <- firstResponse.code match {
          case Forbidden | Unauthorized | NotFound =>
            log.warn("authentication failed, assuming credentials are expired, will retry, possibly refreshing them...") *>
              log.debug(s"response was: ${firstResponse.body}") *>
              auth.refreshSecret(secretCacheRef) *>
              secretCacheRef.get.flatMap(authenticateAndSendHelper)
          case _ => ZIO.succeed(firstResponse) <* log.debug("authentication succeeded: proceeding as normal")
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
            ZIO.fail(TamerError("request failed, giving up."))
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
      currentState: SV,
      queue: Enqueue[NonEmptyChunk[(K, V)]]
  ): RIO[R with SttpClient with EphemeralSecretCache, SV] = for {
    log         <- logTask
    tokenCache  <- ZIO.service[EphemeralSecretCache]
    decodedPage <- fetchAndDecodePage(queryFor(currentState), tokenCache, log)
    chunk = Chunk.fromIterable(filterPage(decodedPage, currentState).map(value => recordKey(currentState, value) -> value))
    _         <- NonEmptyChunk.fromChunk(chunk).map(queue.offer).getOrElse(ZIO.unit)
    nextState <- stateFold(decodedPage, currentState)
  } yield nextState
}

object RESTSetup {
  def apply[R, K: Tag, V: Tag, SV: Tag: Hashable](initialState: SV)(
      query: SV => SttpRequest,
      pageDecoder: String => RIO[R, DecodedPage[V, SV]],
      recordKey: (SV, V) => K,
      stateFold: (DecodedPage[V, SV], SV) => URIO[R, SV],
      authentication: Option[Authentication[R]] = None,
      filterPage: (DecodedPage[V, SV], SV) => List[V] = (dp: DecodedPage[V, SV], _: SV) => dp.data,
      retrySchedule: Option[
        SttpRequest => Schedule[Any, FallibleResponse, FallibleResponse]
      ] = None
  )(
      implicit ev: SerdesProvider[K, V, SV]
  ): RESTSetup[R, K, V, SV] = new RESTSetup(
    initialState,
    recordKey,
    authentication,
    query,
    pageDecoder,
    filterPage,
    stateFold,
    retrySchedule
  ) {}

  def paginated[R, K: Tag, V: Tag](
      baseUrl: String,
      pageDecoder: String => RIO[R, DecodedPage[V, Offset]],
      authentication: Option[Authentication[R]] = None,
      retrySchedule: Option[SttpRequest => Schedule[Any, FallibleResponse, FallibleResponse]] = None
  )(
      recordKey: (Offset, V) => K,
      offsetParameterName: String = "page",
      increment: Int = 1,
      fixedPageElementCount: Option[Int] = None,
      readRequestTimeout: Duration = 30.seconds,
      initialOffset: Offset = Offset(0, 0)
  )(
      implicit ev: SerdesProvider[K, V, Offset]
  ): RESTSetup[R, K, V, Offset] = {
    def queryFor(state: Offset) =
      basicRequest.get(uri"$baseUrl".addParam(offsetParameterName, state.offset.toString)).readTimeout(readRequestTimeout.asScala)

    def nextPageOrNextIndexIfPageNotComplete(decodedPage: DecodedPage[V, Offset], offset: Offset): URIO[R, Offset] = {
      val fetchedElementCount = decodedPage.data.length
      val nextElementIndex    = decodedPage.data.length
      lazy val defaultNextState = fixedPageElementCount match {
        case Some(expectedElemCount) if fetchedElementCount <= expectedElemCount - 1 => ZIO.succeed(offset.nextIndex(nextElementIndex))
        case _                                                                       => ZIO.succeed(offset.incrementedBy(increment))
      }
      decodedPage.nextState.map(ZIO.succeed(_)).getOrElse(defaultNextState)
    }

    def filterPage(decodedPage: DecodedPage[V, Offset], offset: Offset): List[V] = {
      val previousElementCount = offset.nextIndex
      decodedPage.data.drop(previousElementCount)
    }

    new RESTSetup(
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
      ): RIO[R with SttpClient with EphemeralSecretCache, Offset] = for {
        log         <- logTask
        tokenCache  <- ZIO.service[EphemeralSecretCache]
        decodedPage <- fetchWaitingNewEntries(currentState, log, tokenCache)
        chunk = Chunk.fromIterable(this.filterPage(decodedPage, currentState).map(value => this.recordKey(currentState, value) -> value))
        _         <- NonEmptyChunk.fromChunk(chunk).map(queue.offer).getOrElse(ZIO.unit)
        nextState <- stateFold(decodedPage, currentState)
      } yield nextState

      private final def fetchWaitingNewEntries(
          currentState: Offset,
          log: LogWriter[Task],
          tokenCache: EphemeralSecretCache
      ): RIO[R with SttpClient, DecodedPage[V, Offset]] =
        fetchAndDecode(currentState, tokenCache, log).repeat(
          Schedule.exponential(500.millis) *> Schedule.recurWhile(_.data.isEmpty) // FIXME this could never return and leaves tamer hanging
        )

      private final def fetchAndDecode(
          currentState: Offset,
          tokenCache: EphemeralSecretCache,
          log: LogWriter[Task]
      ): RIO[R with SttpClient, DecodedPage[V, Offset]] =
        for {
          decodedPage <- fetchAndDecodePage(this.queryFor(currentState), tokenCache, log)
          dataFromIndex = decodedPage.data.drop(currentState.nextIndex)
          pageLength    = decodedPage.data.length
          _ <- log.debug(s"retrieved ${dataFromIndex.length} new datapoints. ($pageLength total for page ${currentState.offset})")
        } yield decodedPage
    }
  }

  def periodicallyPaginated[R, K: Tag, V: Tag](
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
      retrySchedule: Option[SttpRequest => Schedule[Any, FallibleResponse, FallibleResponse]] = None
  )(recordKey: (PeriodicOffset, V) => K)(
      implicit ev: SerdesProvider[K, V, PeriodicOffset]
  ): RESTSetup[R, K, V, PeriodicOffset] = {
    def queryFor(state: PeriodicOffset) =
      basicRequest.get(uri"$baseUrl".addParam(offsetParameterName, state.offset.toString)).readTimeout(readRequestTimeout.asScala)

    def getNextState(decodedPage: DecodedPage[V, PeriodicOffset], periodicOffset: PeriodicOffset): URIO[R, PeriodicOffset] =
      decodedPage.nextState.map(ZIO.succeed(_)).getOrElse {
        for {
          now <- Clock.instant
          newOffset <-
            if (
              now.isAfter(periodicOffset.periodStart.plus(maxPeriod)) ||
              (decodedPage.data.isEmpty && now.isAfter(periodicOffset.periodStart.plus(minPeriod)))
            ) {
              ZIO.succeed(PeriodicOffset(offset = startingOffset, periodStart = now))
            } else if (decodedPage.data.isEmpty) {
              val nextPeriodStart: Instant = periodicOffset.periodStart.plus(minPeriod)
              ZIO.succeed(PeriodicOffset(offset = startingOffset, periodStart = nextPeriodStart))
            } else {
              ZIO.succeed(periodicOffset.incrementedBy(increment))
            }
        } yield newOffset
      }

    new RESTSetup(
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
      ): RIO[R with SttpClient with EphemeralSecretCache, PeriodicOffset] = for {
        log        <- logTask
        tokenCache <- ZIO.service[EphemeralSecretCache]
        now        <- Clock.instant
        delayUntilNextPeriod <-
          if (currentState.periodStart.isBefore(now))
            ZIO.succeed(Duration.Zero)
          else
            ZIO
              .succeed(Duration.fromInterval(now, currentState.periodStart))
              .tap(delayUntilNextPeriod => log.info(s"$baseUrl is going to sleep for ${delayUntilNextPeriod.render}"))
        decodedPage <- fetchAndDecodePage(this.queryFor(currentState), tokenCache, log).delay(delayUntilNextPeriod)
        chunk = Chunk.fromIterable(this.filterPage(decodedPage, currentState).map(value => this.recordKey(currentState, value) -> value))
        _         <- NonEmptyChunk.fromChunk(chunk).map(queue.offer).getOrElse(ZIO.unit)
        nextState <- this.stateFold(decodedPage, currentState)
      } yield nextState
    }
  }
}
