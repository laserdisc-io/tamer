package tamer
package rest
package support

import com.comcast.ip4s._
import fs2.io.net.Network
import io.circe.literal._
import log.effect.zio.ZioLogWriter.log4sFromClass
import org.http4s._
import org.http4s.AuthScheme.{Basic, Bearer}
import org.http4s.Credentials.Token
import org.http4s.circe._
import org.http4s.dsl.Http4sDslBinCompat
import org.http4s.ember.server._
import org.http4s.headers.Authorization
import org.http4s.implicits._
import org.http4s.server.middleware.Logger
import zio._
import zio.interop.catz._

import scala.util.Try
import scala.annotation.nowarn

object RESTServer extends ZIOAppDefault {
  val randomData = Random.nextInt.map(i => json"""{"rubbish": "...", "data": ${i.toString}}""")

  val rotatingSecret    = Ref.make(0).tap(_.update(_ + 1).schedule(Schedule.spaced(2.seconds)).delay(2.seconds).fork)
  val finitePagination  = Ref.make(List(1 to 20: _*)).tap(_.update(l => l :+ (l.last + 1)).repeat(Schedule.spaced(2.seconds)).fork)
  val dynamicPagination = Ref.make(List.fill(5)(0)).tap(_.update(l => l.map(_ + 1)).repeat(Schedule.spaced(2.seconds)).fork)

  object IsInt { def unapply(s: String): Option[Int] = Try(Integer.valueOf(s).toInt).toOption }

  implicit val network = Network.forAsync[Task]

  object TaskDsl extends Http4sDslBinCompat[Task]
  import TaskDsl._

  def server(rotatingSecret: Ref[Int], finitePagination: Ref[List[Int]], dynamicPagination: Ref[List[Int]]): EmberServerBuilder[Task] = {
    val forbidden: UIO[Response[Task]] = ZIO.succeed(Response(Forbidden))

    def checkAuth(r: Request[Task], scheme: AuthScheme, token: String) = r.headers.get[Authorization].exists(_.credentials == Token(scheme, token))
    def checkBasicAuth(r: Request[Task])                               = checkAuth(r, Basic, "dXNlcjpwYXNz") // user:pass in b64
    def checkBearerAuthZIO(r: Request[Task])                           = rotatingSecret.get.map(ct => checkAuth(r, Bearer, s"validToken$ct"))

    val routes = HttpRoutes.of[Task] {
      case r @ GET -> Root / "auth" if checkBasicAuth(r)       => rotatingSecret.get.flatMap(currentToken => Ok("validToken" + currentToken))
      case r @ GET -> Root / "basic-auth" if checkBasicAuth(r) => randomData.flatMap(Ok(_))
      case r @ GET -> Root                                     => ZIO.ifZIO(checkBearerAuthZIO(r))(randomData.flatMap(Ok(_)), forbidden)
      case r @ GET -> Root / "finite-pagination" =>
        @nowarn val Some(page) = r.uri.query.pairs.collectFirst { case (_, Some(IsInt(p))) => p }
        val bodyZIO            = finitePagination.get.map(_.grouped(3).toList.lift(page)).map(_.map(_.mkString(",")).getOrElse(""))
        bodyZIO.flatMap(Ok(_)) // outputs "1,2,3" "4,5,6" and so on...
      case r @ GET -> Root / "dynamic-pagination" =>
        @nowarn val Some(page) = r.uri.query.pairs.collectFirst { case (_, Some(IsInt(p))) => p }
        val bodyZIO            = dynamicPagination.get.map(_.grouped(3).toList.lift(page)).map(_.map(_.mkString(",")).getOrElse(""))
        bodyZIO.flatMap(Ok(_)) // outputs "1,2,3" "4,5,6" and so on...
    }

    val logger        = log4sFromClass.provideEnvironment(ZEnvironment(this.getClass))
    val loggingRoutes = Logger.httpRoutes[Task](true, true, _ => false, Some(s => logger.flatMap(_.info(s))))(routes)

    EmberServerBuilder.default[Task].withHost(ipv4"0.0.0.0").withPort(port"9395").withHttpApp(loggingRoutes.orNotFound)
  }

  override final val run =
    (rotatingSecret <&> finitePagination <&> dynamicPagination).flatMap { case (rs, fp, dp) => server(rs, fp, dp).build.useForever.exitCode }
}
