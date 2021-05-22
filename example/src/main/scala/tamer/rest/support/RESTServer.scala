package tamer
package rest
package support

import uzhttp.HTTPError.Forbidden
import uzhttp.Response
import uzhttp.server.Server
import zio._
import zio.duration.durationInt
import zio.random.Random

import java.net.InetSocketAddress

import scala.annotation.nowarn

object RESTServer extends App {
  val rotatingSecret = for {
    secret <- Ref.make(0)
    _      <- secret.update(_ + 1).schedule(Schedule.spaced(2.seconds)).delay(2.seconds).fork
  } yield secret
  val jsonHeader            = List("content-type" -> "application/json")
  val respondWithRandomData = random.nextInt.map(i => Response.plain(s"""{"rubbish":"...","data":"$i"}""", headers = jsonHeader))
  val finitePagination = for {
    pages <- Ref.make(List(1 to 20: _*))
    _     <- pages.update(l => l :+ (l.last + 1)).repeat(Schedule.spaced(2.seconds)).fork
  } yield pages
  val dynamicPagination = for {
    pages <- Ref.make(List.fill(5)(0))
    _     <- pages.update(l => l.map(_ + 1)).repeat(Schedule.spaced(2.seconds)).fork
  } yield pages

  def server(rotatingSecret: Ref[Int], finitePagination: Ref[List[Int]], dynamicPagination: Ref[List[Int]]): Server.Builder[Random] =
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
        val bodyM = finitePagination.get.map(_.grouped(3).toList.lift(page)).map(_.map(_.mkString(",")).getOrElse(""))
        bodyM.map(Response.plain(_)) // outputs "1,2,3" "4,5,6" and so on...
      case req if req.uri.getPath == "/dynamic-pagination" =>
        val page  = req.uri.getQuery.dropWhile(c => !c.isDigit).toInt
        val bodyM = dynamicPagination.get.map(_.grouped(3).toList.lift(page)).map(_.map(_.mkString(",")).getOrElse(""))
        bodyM.map(Response.plain(_))
      case _ => ZIO.fail(Forbidden(s"You don't have access to this resource."))
    }

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = for {
    rotatingSecret    <- rotatingSecret
    finitePagination  <- finitePagination
    dynamicPagination <- dynamicPagination
    exit              <- server(rotatingSecret, finitePagination, dynamicPagination).serve.useForever.exitCode
  } yield exit
}
