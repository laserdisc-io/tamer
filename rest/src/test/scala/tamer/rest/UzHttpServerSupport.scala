package tamer
package rest

import java.net.InetSocketAddress
import java.util.UUID

import io.circe.syntax._
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.client3.UriContext
import uzhttp.{Request, Response}
import uzhttp.HTTPError.{Forbidden, NotFound}
import uzhttp.Request.Method.POST
import uzhttp.server.Server
import zio._

import zio.Clock
import zio.stream.ZTransducer
import zio.test._
import zio.{ Random, Random }

trait UzHttpServerSupport {
  case class ServerLog(lastRequest: Option[Long])

  private[this] def serverBuilder(port: Int, sl: Ref[ServerLog]): Server.Builder[Any] = {
    val jsonHeaders = List("content-type" -> "application/json")

    def randomJson(r: Request) = for {
      log  <- log4sFromName.orDie
      now  <- ZIO.succeed(System.nanoTime())
      uuid <- ZIO.succeed(UUID.randomUUID())
      _    <- log.info(s"got ${r.method.name} request on ${r.uri}").orDie
      _    <- sl.update(_.copy(lastRequest = Some(now)))
      out  <- ZIO.succeed(Response.plain(Value(now, uuid).asJson.spaces2, headers = jsonHeaders))
    } yield out

    def paginatedJson(r: Request) = for {
      log  <- log4sFromName.orDie
      page <- ZIO.succeed(uri"${r.uri.toString}".paramsMap("page").toLong)
      uuid <- ZIO.succeed(UUID.randomUUID())
      _    <- log.info(s"got ${r.method.name} request ${r.uri}").orDie
      _    <- sl.update(_.copy(lastRequest = Some(page)))
      out  <- ZIO.succeed(Response.plain(Value(page, uuid).asJson.spaces2, headers = jsonHeaders))
    } yield out

    def authChallenge(r: Request) = for {
      log  <- log4sFromName.orDie
      body <- r.body.map(_.transduce(ZTransducer.utf8Decode).runCollect.map(_.mkString)).getOrElse(ZIO.succeed(""))
      out <-
        if (r.headers.get("header1").contains("value1") && body == "valid body") {
          log.info(s"got ${r.method.name} request on ${r.uri} with correct header ${r.headers("header1")} and payload '$body'").orDie *>
            ZIO.succeed(Response.plain("valid-token"))
        } else {
          log.info(s"got ${r.method.name} request on ${r.uri} that is invalid, failing the fiber").orDie *>
            ZIO.fail(Forbidden("No access"))
        }
    } yield out

    def checkAuth(r: Request) = r.headers.get("Authorization").contains("Bearer valid-token")

    Server.builder(new InetSocketAddress("0.0.0.0", port)).handleSome {
      case r if r.uri.getPath == "/random"                                => randomJson(r).provideService(s"fake-random-$port")
      case r if r.uri.getPath == "/paginated"                             => paginatedJson(r).provideService(s"fake-paginated-$port")
      case r if r.uri.getPath == "/auth-request-form" && r.method == POST => authChallenge(r).provideService(s"fake-auth-request-form-$port")
      case r if r.uri.getPath == "/auth-token" && checkAuth(r)            => randomJson(r).provideService(s"fake-auth-token-$port")
      case r if r.uri.getPath == "/auth-token"                            => ZIO.fail(Forbidden("No access"))
      case _                                                              => ZIO.fail(NotFound("No such resource"))
    }
  }

  final def withServer[R](f: (Int, Ref[ServerLog]) => RIO[R, TestResult]): RIO[R with Blocking with Clock with Random, TestResult] = for {
    serverLog <- Ref.make(ServerLog(None))
    port      <- Random.nextIntBetween(10000, 50000)
    out       <- serverBuilder(port, serverLog).serve.use(_ => f(port, serverLog))
  } yield out
}
