package tamer.rest

import com.sksamuel.avro4s.Codec
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.client3.httpclient.zio.{SttpClient, send}
import sttp.client3.{Request, Response, UriContext, basicRequest}
import sttp.model.StatusCode.{Forbidden, NotFound, Unauthorized}
import sttp.model.Method
import tamer.config.KafkaConfig
import tamer.job.AbstractTamerJob
import tamer.{AvroCodec, HashableState, TamerError}
import zio.{Chunk, Has, Queue, RIO, Ref, Task, UIO, ZEnv, ZIO, ZLayer}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.hashing.MurmurHash3.stringHash

//case class Authentication/
trait Authentication[-R] {
  val secretM: UIO[Ref[Option[String]]] = Ref.make(Option.empty[String])

  def addAuthentication(request: SttpRequest, secret: String): SttpRequest

  def setSecret(secretRef: Ref[Option[String]]): ZIO[R, TamerError, Unit]

  def refreshSecret(secretRef: Ref[Option[String]]): ZIO[R, TamerError, Unit] = setSecret(secretRef)
  // TODO: widen error since this is user space
}

object Authentication {
  def basic[R](username: String, password: String, method: Method = Method.GET): Authentication[R] = ???
  //    case class Basic(username: String, password: String) extends Authentication
  //    case class Token(bearer: String) extends Authentication
}

object TamerRestJob {
  case class Offset(offset: Int, token: Option[String]) {
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
    R <: ZEnv with SttpClient with KafkaConfig with Has[Ref[Option[String]]], // TODO: use alias type
    K: Codec,
    V: Codec,
    S: Codec
  ](
     setup: RestConfiguration[R, K, V, S]
   ) = new TamerRestJob[R, K, V, S](setup)

  def withPagination[
    R <: ZEnv with SttpClient with KafkaConfig with Has[Ref[Option[String]]], // TODO: use type alias
    K <: Product : Codec,
    V <: Product : Codec
  ](
     baseUrl: String,
     pageDecoder: String => RIO[R, DecodedPage[V, Offset]],
     offsetParameterName: String = "page",
     increment: Int = 1,
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

    val transitions: RestConfiguration.State[K, V, Offset] =
      RestConfiguration.State(Offset(0, None))(s => UIO(s.incrementedBy(increment)), deriveKafkaRecordKey)

    new TamerRestJob[R, K, V, Offset](RestConfiguration(queryBuilder = queryBuilder, transitions = transitions, pageDecoder = pageDecoder))
  }
}


class TamerRestJob[
  -R <: ZEnv with SttpClient with KafkaConfig with Has[Ref[Option[String]]], // TODO: use alias type
  K: Codec,
  V: Codec,
  S: Codec
](
   setup: RestConfiguration[R, K, V, S]
 ) extends AbstractTamerJob[R, K, V, S](setup.generic) {
  val tokenCacheM: UIO[Ref[Option[String]]] = Ref.make[Option[String]](None)
  type TokenCache = Has[Ref[Option[String]]]
  val tokenCache: ZLayer[Any, Nothing, Has[Ref[Option[String]]]] = ZLayer.fromEffect(tokenCacheM <* UIO(println("-" * 200 + "Instantiating token cache (should happen once per job" + "-" * 200))) // TODO: remove println

  private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.rest")

  private def fetchAndDecodePage(request: SttpRequest, tokenCacheRef: Ref[Option[String]], log: LogWriter[Task]): RIO[R, DecodedPage[V, S]] = {
    def authenticateAndSendRequest(requestToTransform: SttpRequest, tokenCacheRef: Ref[Option[String]], log: LogWriter[Task]): ZIO[R, Throwable, Response[Either[String, String]]] = {
      val auth = setup.queryBuilder.authentication.get // FIXME: unsafe

      def authenticateAndSendHelper(maybeToken: Option[String]) = {
        val authenticatedRequest = maybeToken.map(token => auth.addAuthentication(requestToTransform, token)).getOrElse(requestToTransform)
        log.debug(s"Going to execute request $authenticatedRequest") *> send(authenticatedRequest)
      }

      for {
        maybeToken <- tokenCacheRef.get
        _ <- log.info(s"Currently the token cache contains: $maybeToken")
        _ <- maybeToken match {
          case Some(_) => UIO(()) // secret is already present do nothing
          case None => auth.setSecret(tokenCacheRef) <* log.info("Got secret for the first time.")
        }
        firstResponse <- authenticateAndSendHelper(maybeToken)
        response <- firstResponse.code match {
          case Forbidden | Unauthorized | NotFound =>
            log.warn("Authentication failed, assuming credencial are expired, refresh and retry...") *>
              log.warn(s"Response was: ${firstResponse.body}") *>
              auth.refreshSecret(tokenCacheRef) *>
              tokenCacheRef.get.flatMap(authenticateAndSendHelper)
          case _ => UIO(firstResponse) <* log.info("Authentication succeeded: proceeding as normal") // TODO: remove or decrease level
        }
      } yield response
    }

    for {
      response <- setup.queryBuilder.authentication match {
        case Some(_) => authenticateAndSendRequest(request, tokenCacheRef, log)
        case None => send(request)
      }
      decodedPage <- response.body match {
        case Left(error) =>
          // assume the cookie/auth expired
          log.error(s"""Request failed with error "$error"""") *>
            tokenCacheRef.set(None) *> // clear cache
            RIO.fail(TamerError("Request failed, giving up."))
        //fetchAndDecodePage(request, tokenCacheRef, log)
        case Right(body) =>
          setup.pageDecoder(body)
      }
    } yield decodedPage
  }

  override protected def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] = {
    val logic: ZIO[R with SttpClient with TokenCache, Throwable, S] = for {
      log <- logTask
      tokenCache <- ZIO.service[Ref[Option[String]]]
      request = setup.queryBuilder.query(currentState)
      decodedPage <- fetchAndDecodePage(request, tokenCache, log)
      _ <- ZIO.foreach(decodedPage.data.map(value => (setup.transitions.deriveKafkaRecordKey(currentState, value), value)))(c => q.offer(Chunk(c)))
      nextState <- setup.transitions.getNextState.apply(decodedPage.nextState.getOrElse(currentState))
    } yield nextState

    logic.mapError(e => TamerError(e.getLocalizedMessage, e))
  }
}
