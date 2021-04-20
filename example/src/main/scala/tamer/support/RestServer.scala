package tamer.support

import uzhttp.HTTPError.Forbidden
import uzhttp.Response
import uzhttp.server.Server
import zio.clock.Clock
import zio.duration.durationInt
import zio.random.Random
import zio.{ExitCode, Ref, Schedule, UIO, URIO, ZIO, random}

import java.net.InetSocketAddress

object RestServer extends zio.App {
  val rotatingSecretM: URIO[Clock, Ref[Int]] = for {
    secret <- Ref.make(0)
    _ <- secret.update(_ + 1).schedule(Schedule.spaced(2.seconds)).delay(2.seconds).fork
  } yield secret
  val jsonHeader = List("content-type" -> "application/json")

  def server(rotatingSecret: Ref[Int]): Server.Builder[Random] = Server.builder(new InetSocketAddress("0.0.0.0", 9095)).handleSome {
    case req if req.uri.getPath == "/auth" && req.headers.get("Authorization").contains("Basic dXNlcjpwYXNz") => // user:pass
      rotatingSecret.get.flatMap(currentToken => UIO(Response.plain("validToken" + currentToken)))
    case req if req.uri.getPath == "/" =>
      rotatingSecret.get.flatMap { currentToken =>
        if (req.headers.get("Authorization").contains("Bearer validToken" + currentToken)) {
          random.nextInt.map(i => Response.plain(s"""{"rubbish":"...","data":"$i"}""", headers = jsonHeader))
        } else {
          ZIO.fail(Forbidden(s"Invalid token presented: ${req.headers.getOrElse("Authorization", "`no token was presented`")}"))
        }
      }
    case _ => ZIO.fail(Forbidden(s"You don't have access to this resource."))
  }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    for {
      rotatingSecret <- rotatingSecretM
      exit <- server(rotatingSecret).serve.useForever.exitCode
    } yield exit
  }
}
