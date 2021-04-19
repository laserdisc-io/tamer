package tamer.support

import uzhttp.HTTPError.Forbidden
import uzhttp.Response
import uzhttp.server.Server
import zio.random.Random
import zio.{ExitCode, UIO, URIO, ZIO, random}

import java.net.InetSocketAddress

object RestServer extends zio.App {
  val jsonHeader = List("content-type" -> "application/json")
  val server: Server.Builder[Random] = Server.builder(new InetSocketAddress("0.0.0.0", 9095)).handleSome {
    case req if req.uri.getPath == "/auth" && req.headers.get("Authorization").contains("Basic dXNlcjpwYXNz") => // user:pass
      UIO(Response.plain("validToken"))
    case req if req.uri.getPath == "/" && req.headers.get("Authorization").contains("Bearer validToken") =>
      random.nextInt.map(i => Response.plain(s"""{"rubbish":"...","data":"$i"}""", headers = jsonHeader))
    case _ => ZIO.fail(Forbidden(s"You don't have access to this resource."))
  }

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = server.serve.useForever.exitCode
}
