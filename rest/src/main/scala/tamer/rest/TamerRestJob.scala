package tamer.rest

import com.sksamuel.avro4s.Codec
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.capabilities.zio.ZioStreams
import sttp.capabilities.{Effect, WebSockets}
import sttp.client3.httpclient.zio.{SttpClient, send}
import sttp.client3.{Request, UriContext, basicRequest}
import sttp.model.Method
import tamer.config.KafkaConfig
import tamer.job.AbstractTamerJob
import tamer.{AvroCodec, HashableState, TamerError}
import zio.{Chunk, Has, Queue, RIO, Ref, Task, UIO, ZEnv, ZIO, ZLayer}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.hashing.MurmurHash3.stringHash

trait Authentication[-R] {
  def requestTransform(authInfo: Option[String]): ZIO[R, TamerError, (Request[Either[String, String], ZioStreams with Effect[Task] with WebSockets] => Request[Either[String, String], ZioStreams with Effect[Task] with WebSockets], Option[String])]
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
    R <: ZEnv with SttpClient with KafkaConfig,
    K: Codec,
    V: Codec,
    S: Codec
  ](
     setup: RestConfiguration[R, K, V, S]
   ) = new TamerRestJob[R, K, V, S](setup)

  def withPagination[
    R <: ZEnv with SttpClient with KafkaConfig,
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
  -R <: ZEnv with SttpClient with KafkaConfig,
  K: Codec,
  V: Codec,
  S: Codec
](
   setup: RestConfiguration[R, K, V, S]
 ) extends AbstractTamerJob[R, K, V, S](setup.generic) {
  val tokenCacheM: UIO[Ref[Option[String]]] = Ref.make[Option[String]](None)
  type TokenCache = Has[Ref[Option[String]]]
  val tokenCache: ZLayer[Any, Nothing, Has[Ref[Option[String]]]] = ZLayer.fromEffect(tokenCacheM)

  private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.rest")

  private def authenticateAndSendRequest(requestToTransform: Request[Either[String, String], ZioStreams with Effect[Task] with WebSockets], tokenCacheRef: Ref[Option[String]], log: LogWriter[Task]) = for {
    tokenCache <- tokenCacheRef.get
    authenticatedRequest <- setup.queryBuilder.authentication.map(_.requestTransform(tokenCache)).map(_.map(_._1.apply(requestToTransform))).getOrElse(UIO(requestToTransform))
    _ <- log.debug(s"Going to execute request $authenticatedRequest")
    response <- send(authenticatedRequest)
  } yield response

  private def fetchAndDecodePage(request: Request[Either[String, String], ZioStreams with Effect[Task] with WebSockets], tokenCacheRef: Ref[Option[String]], log: LogWriter[Task]): RIO[R, DecodedPage[V, S]] = {
    for {
      response <- authenticateAndSendRequest(request, tokenCacheRef, log)
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
      decodedPage <- fetchAndDecodePage(request, tokenCache, log).retryN(2)
      _ <- ZIO.foreach(decodedPage.data.map(value => (setup.transitions.deriveKafkaRecordKey(currentState, value), value)))(c => q.offer(Chunk(c)))
      nextState <- setup.transitions.getNextState.apply(decodedPage.nextState.getOrElse(currentState))
    } yield nextState

    logic.mapError(e => TamerError(e.getLocalizedMessage, e)).provideSomeLayer[R with SttpClient](tokenCache)
  }
}
