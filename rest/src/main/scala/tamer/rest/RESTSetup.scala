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

trait QueryBuilder[-R, -S] {

  /** Used for hashing purposes
    */
  val queryId: Int

  def query(state: S): Request[Either[String, String], ZioStreams with Effect[Task] with WebSockets]

  val authentication: Option[Authentication[R]] = None
}

final case class DecodedPage[V, S](data: List[V], nextState: Option[S])
object DecodedPage {
  def fromString[R, V, S](decoder: String => RIO[R, List[V]]): String => RIO[R, DecodedPage[V, S]] =
    decoder.andThen(_.map(DecodedPage(_, None)))
}

sealed abstract case class RESTSetup[-R, K, V, S: Hashable](
    serdes: Setup.Serdes[K, V, S],
    initialState: S,
    recordKey: (S, V) => K,
    queryBuilder: QueryBuilder[R, S],
    pageDecoder: String => RIO[R, DecodedPage[V, S]],
    filterPage: (DecodedPage[V, S], S) => List[V],
    stateFold: (DecodedPage[V, S], S) => URIO[R, S]
) extends Setup[R with SttpClient with Has[EphemeralSecretCache], K, V, S] {
  override final val stateKey = queryBuilder.queryId + initialState.hash
  override final val repr =
    s"""query:             ${queryBuilder.query(initialState)}
       |query id:          ${queryBuilder.queryId}
       |initial state:     $initialState
       |initial state id:  ${initialState.hash}
       |initial state key: $stateKey
       |""".stripMargin

  protected final val logTask = log4sFromName.provide("tamer.rest")

  protected def fetchAndDecodePage(
      request: SttpRequest,
      secretCacheRef: EphemeralSecretCache,
      log: LogWriter[Task]
  ): RIO[R with SttpClient, DecodedPage[V, S]] = {
    def authenticateAndSendRequest(auth: Authentication[R]): RIO[R with SttpClient, Response[Either[String, String]]] = {

      def authenticateAndSendHelper(maybeToken: Option[String]): RIO[SttpClient, Response[Either[String, String]]] = {
        val authenticatedRequest = auth.addAuthentication(request, maybeToken)
        log.debug(s"going to execute request $authenticatedRequest") *> send(authenticatedRequest)
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
      response <- queryBuilder.authentication.map(authenticateAndSendRequest).getOrElse(send(request))
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
  override def iteration(currentState: S, queue: Queue[Chunk[(K, V)]]): RIO[R with SttpClient with Has[EphemeralSecretCache], S] = for {
    log         <- logTask
    tokenCache  <- ZIO.service[EphemeralSecretCache]
    decodedPage <- fetchAndDecodePage(queryBuilder.query(currentState), tokenCache, log)
    chunk = Chunk.fromIterable(filterPage(decodedPage, currentState).map(value => recordKey(currentState, value) -> value))
    _         <- queue.offer(chunk)
    nextState <- stateFold(decodedPage, currentState)
  } yield nextState
}

object RESTSetup {
  def apply[R, K: Codec, V: Codec, S: Codec: Hashable](
      queryBuilder: QueryBuilder[R, S],
      pageDecoder: String => RIO[R, DecodedPage[V, S]],
      initialState: S,
      recordKey: (S, V) => K,
      stateFold: (DecodedPage[V, S], S) => URIO[R, S],
      filterPage: (DecodedPage[V, S], S) => List[V] = (dp: DecodedPage[V, S], _: S) => dp.data
  ): RESTSetup[R, K, V, S] = new RESTSetup(
    Setup.Serdes[K, V, S],
    initialState,
    recordKey,
    queryBuilder,
    pageDecoder,
    filterPage,
    stateFold
  ) {}

  def paginated[R, K: Codec, V: Codec](
      baseUrl: String,
      pageDecoder: String => RIO[R, DecodedPage[V, Offset]],
      authenticationMethod: Option[Authentication[R]] = None
  )(
      recordKey: (Offset, V) => K,
      offsetParameterName: String = "page",
      increment: Int = 1,
      fixedPageElementCount: Option[Int] = None,
      readRequestTimeout: Duration = 30.seconds,
      initialOffset: Offset = Offset(0, 0)
  )(
      implicit ev: Codec[Offset]
  ): RESTSetup[R with Clock, K, V, Offset] = {
    val queryBuilder: QueryBuilder[R, Offset] = new QueryBuilder[R, Offset] {

      /** Used for hashing purposes
        */
      override val queryId: Int = (baseUrl + offsetParameterName).hash + increment

      override def query(state: Offset): Request[Either[String, String], Any] =
        basicRequest
          .get(uri"$baseUrl".addParam(offsetParameterName, state.offset.toString))
          .readTimeout(readRequestTimeout.asScala)

      override val authentication: Option[Authentication[R]] = authenticationMethod
    }

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
      Setup.Serdes[K, V, Offset],
      initialOffset,
      recordKey,
      queryBuilder,
      pageDecoder,
      filterPage,
      nextPageOrNextIndexIfPageNotComplete
    ) {
      override def iteration(
          currentState: Offset,
          queue: Queue[Chunk[(K, V)]]
      ): RIO[R with Clock with SttpClient with Has[EphemeralSecretCache], Offset] = for {
        log         <- logTask
        tokenCache  <- ZIO.service[EphemeralSecretCache]
        decodedPage <- fetchWaitingNewEntries(currentState, log, tokenCache)
        chunk = Chunk.fromIterable(filterPage(decodedPage, currentState).map(value => recordKey(currentState, value) -> value))
        _         <- queue.offer(chunk)
        nextState <- stateFold(decodedPage, currentState)
      } yield nextState

      private final def fetchWaitingNewEntries(
          currentState: Offset,
          log: LogWriter[Task],
          tokenCache: EphemeralSecretCache
      ): RIO[R with Clock with SttpClient, DecodedPage[V, Offset]] =
        fetchAndDecode(currentState, tokenCache, log).repeat(
          Schedule.exponential(500.millis) *> Schedule.recurWhile(_.data.isEmpty)
        )

      private final def fetchAndDecode(
          currentState: Offset,
          tokenCache: EphemeralSecretCache,
          log: LogWriter[Task]
      ): RIO[R with Clock with SttpClient, DecodedPage[V, Offset]] =
        for {
          decodedPage <- fetchAndDecodePage(queryBuilder.query(currentState), tokenCache, log)
          dataFromIndex = decodedPage.data.drop(currentState.nextIndex)
          pageLength    = decodedPage.data.length
          _ <- log.debug(s"Fetch and Decode retrieved ${dataFromIndex.length} new datapoints. ($pageLength total for page ${currentState.offset})")
        } yield decodedPage
    }
  }

  def periodicallyPaginated[R, K: Codec, V: Codec](
      baseUrl: String,
      pageDecoder: String => RIO[R, DecodedPage[V, PeriodicOffset]],
      periodStart: Instant,
      offsetParameterName: String = "page",
      increment: Int = 1,
      authenticationMethod: Option[Authentication[R]] = None,
      minPeriod: Duration = 5.minutes,
      maxPeriod: Duration = 1.hour,
      readRequestTimeout: Duration = 30.seconds,
      startingOffset: Int = 0,
      filterPage: (DecodedPage[V, PeriodicOffset], PeriodicOffset) => List[V] = (dp: DecodedPage[V, PeriodicOffset], _: PeriodicOffset) => dp.data
  )(recordKey: (PeriodicOffset, V) => K)(
      implicit ev: Codec[PeriodicOffset]
  ): RESTSetup[R with Clock, K, V, PeriodicOffset] = {
    val queryBuilder = new QueryBuilder[R, PeriodicOffset] {

      /** Used for hashing purposes
        */
      override val queryId: Int = (baseUrl + offsetParameterName).hash + increment

      override def query(state: PeriodicOffset): Request[Either[String, String], Any] =
        basicRequest
          .get(uri"$baseUrl".addParam(offsetParameterName, state.offset.toString))
          .readTimeout(readRequestTimeout.asScala)

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

    new RESTSetup(
      Setup.Serdes[K, V, PeriodicOffset],
      PeriodicOffset(startingOffset, periodStart),
      recordKey,
      queryBuilder,
      pageDecoder,
      filterPage,
      getNextState
    ) {
      override def iteration(
          currentState: PeriodicOffset,
          queue: Queue[Chunk[(K, V)]]
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
        decodedPage <- fetchAndDecodePage(queryBuilder.query(currentState), tokenCache, log).delay(delayUntilNextPeriod)
        _           <- queue.offer(Chunk.fromIterable(filterPage(decodedPage, currentState).map(value => recordKey(currentState, value) -> value)))
        nextState   <- getNextState(decodedPage, currentState)
      } yield nextState
    }
  }
}
