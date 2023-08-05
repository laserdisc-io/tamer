package tamer
package rest

import cats.data.NonEmptyList
import com.comcast.ip4s._
import fs2.io.net.Network
import io.circe.syntax._
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import org.http4s._
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s.circe._
import org.http4s.dsl.Http4sDslBinCompat
import org.http4s.ember.server._
import org.http4s.headers.Authorization
import org.http4s.implicits._
import org.http4s.server.middleware.Logger
import org.typelevel.ci._
import zio._
import zio.interop.catz._
import zio.test._

trait HttpServerSupport {

  case class ServerLog(lastRequest: Option[Long])

  private[this] def serverBuilder(port: Port, sl: Ref[ServerLog], log: LogWriter[Task]): EmberServerBuilder[Task] = {

    implicit val network = Network.forAsync[Task]

    object TaskDsl extends Http4sDslBinCompat[Task]
    import TaskDsl._

    val randomJson =
      (Clock.nanoTime.tap(now => sl.set(ServerLog(Some(now)))) <&> Random.nextUUID).flatMap { case (now, uuid) => Ok(Value(now, uuid).asJson) }

    def paginatedJson(p: Long) =
      Random.nextUUID.flatMap(uuid => Ok(Value(p, uuid).asJson)) <& sl.set(ServerLog(Some(p)))

    object PageMatcher extends QueryParamDecoderMatcher[Long]("page")
    def checkCustomHeader(r: Request[Task]) = r.headers.get(ci"header1") == NonEmptyList.one("value1")
    def validBodyOrForbidden(r: Request[Task]): Task[Response[Task]] =
      (r.bodyText.compile.string.filterOrFail(_ == "valid body")(new RuntimeException("invalid body")) *> Ok("valid token"))
        .catchAll(_ => ZIO.succeed(Response(Forbidden)))
    def checkAuthorizationHeader(r: Request[Task]) = r.headers.get[Authorization].exists(_.credentials == Token(Bearer, "valid-token"))

    val routes = HttpRoutes.of[Task] {
      case GET -> Root / "random"                                         => randomJson
      case GET -> Root / "paginated" :? PageMatcher(p)                    => paginatedJson(p)
      case r @ POST -> Root / "auth-request-form" if checkCustomHeader(r) => validBodyOrForbidden(r)
      case POST -> Root / "auth-request-form"                             => ZIO.succeed(Response(Forbidden))
      case r @ GET -> Root / "auth-token" if checkAuthorizationHeader(r)  => randomJson
      case GET -> Root / "auth-token"                                     => ZIO.succeed(Response(Forbidden))
    }

    val loggingRoutes = Logger.httpRoutes[Task](true, true, _ => false, Some(log.info(_)))(routes)

    EmberServerBuilder.default[Task].withHost(ipv4"0.0.0.0").withPort(port).withHttpApp(loggingRoutes.orNotFound)
  }

  final def withServer(f: (Port, Ref[ServerLog]) => Task[TestResult]): Task[TestResult] = for {
    log       <- log4sFromName.provideEnvironment(ZEnvironment("HttpServerSupport"))
    serverLog <- Ref.make(ServerLog(None))
    i         <- Random.nextIntBetween(10000, 50000)
    _         <- log.info(s"trying to start an Ember server on $i")
    port      <- ZIO.fromOption(Port.fromInt(i)).orDieWith(_ => new RuntimeException(s"unable to create port on $i"))
    out       <- serverBuilder(port, serverLog, log).build.use(_ => f(port, serverLog))
  } yield out
}
