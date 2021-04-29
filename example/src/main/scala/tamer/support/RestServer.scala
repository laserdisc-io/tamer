package tamer.support

import uzhttp.HTTPError.Forbidden
import uzhttp.Response
import uzhttp.server.Server
import zio.clock.Clock
import zio.duration.durationInt
import zio.random.Random
import zio.{ExitCode, Ref, Schedule, UIO, URIO, ZIO, random}

import java.net.InetSocketAddress

import scala.annotation.nowarn

object RestServer extends zio.App {
  val rotatingSecretM: URIO[Clock, Ref[Int]] = for {
    secret <- Ref.make(0)
    _      <- secret.update(_ + 1).schedule(Schedule.spaced(2.seconds)).delay(2.seconds).fork
  } yield secret
  val jsonHeader = List("content-type" -> "application/json")
  val respondWithRandomData: URIO[Random, Response] =
    random.nextInt.map(i => Response.plain(s"""{"rubbish":"...","data":"$i"}""", headers = jsonHeader))
  val dynamicPaginationM = for {
    pages <- Ref.make(List(1 to 20: _*))
    _     <- pages.update(l => l :+ (l.last + 1)).repeat(Schedule.spaced(2.seconds)).fork
  } yield pages

  def server(rotatingSecret: Ref[Int], dynamicPagination: Ref[List[Int]]): Server.Builder[Random] =
    Server.builder(new InetSocketAddress("0.0.0.0", 9095)).handleSome {
      case req if req.uri.getPath == "/auth" && req.headers.get("Authorization").contains("Basic dXNlcjpwYXNz") => // user:pass
        rotatingSecret.get.flatMap(currentToken => UIO(Response.plain("validToken" + currentToken))): @nowarn
      case req if req.uri.getPath == "/" =>
        rotatingSecret.get.flatMap { currentToken =>
          if (req.headers.get("Authorization").contains("Bearer validToken" + currentToken)) {
            respondWithRandomData
          } else {
            ZIO.fail(Forbidden(s"Invalid token presented: ${req.headers.getOrElse("Authorization", "`no token was presented`")}"))
          }
        }
      case req if req.uri.getPath == "/basic-auth" && req.headers.get("Authorization").contains("Basic dXNlcjpwYXNz") => // user:pass
        respondWithRandomData
      case req if req.uri.getPath == "/finite-pagination" =>
        val page  = req.uri.getQuery.dropWhile(c => !c.isDigit).toInt
        val bodyM = dynamicPagination.get.map(_.grouped(3).toList.lift(page)).map(_.map(_.mkString(",")).getOrElse(""))
        bodyM.map(Response.plain(_)) // outputs "1,2,3" "4,5,6" and so on...
      case _ => ZIO.fail(Forbidden(s"You don't have access to this resource."))
    }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    for {
      rotatingSecret    <- rotatingSecretM
      dynamicPagination <- dynamicPaginationM
      exit              <- server(rotatingSecret, dynamicPagination).serve.useForever.exitCode
    } yield exit
}
