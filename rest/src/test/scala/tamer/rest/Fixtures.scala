package tamer
package rest

import java.net.InetSocketAddress
import java.util.UUID

import io.circe.generic.semiauto._
import io.circe.syntax._
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.client3.UriContext
import uzhttp.{Request, Response}
import uzhttp.HTTPError.{Forbidden, NotFound}
import uzhttp.Request.Method.POST
import uzhttp.server.Server
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.random.Random
import zio.stream.ZTransducer
import zio.test._

object Fixtures {

  var allowLogin: Boolean = true

  case class Key(time: Long)
  case class Value(time: Long, uuid: UUID)
  object Value {
    implicit val circeCodec = deriveCodec[Value]
  }
  case class State(count: Int)
  object State {
    implicit val hashable: Hashable[State] = _.count
  }

  case class ServerLog(lastRequestTimestamp: Option[Long])

  def serverResource(serverLog: Ref[ServerLog], port: Int) = {
    val jsonHeaders = List("content-type" -> "application/json")

    def randomJson(r: Request) = for {
      log  <- log4sFromName.orDie
      now  <- UIO(System.nanoTime())
      uuid <- UIO(UUID.randomUUID())
      _    <- log.info(s"got ${r.method.name} request on ${r.uri}").orDie
      _    <- serverLog.update(_.copy(lastRequestTimestamp = Some(now)))
      out  <- UIO(Response.plain(Value(now, uuid).asJson.spaces2, headers = jsonHeaders))
    } yield out

    def paginatedJson(r: Request) = for {
      log  <- log4sFromName.orDie
      page <- UIO(uri"${r.uri.toString}".paramsMap("page").toLong)
      uuid <- UIO(UUID.randomUUID())
      _    <- log.info(s"got ${r.method.name} request ${r.uri}").orDie
      _    <- serverLog.update(_.copy(lastRequestTimestamp = Some(page)))
      out  <- UIO(Response.plain(Value(page, uuid).asJson.spaces2, headers = jsonHeaders))
    } yield out

    def authChallenge(r: Request) = for {
      log  <- log4sFromName.orDie
      body <- r.body.map(_.transduce(ZTransducer.utf8Decode).runCollect.map(_.mkString)).getOrElse(UIO(""))
      out <-
        if (r.headers.get("header1").contains("value1") && body == "valid body") {
          log.info(s"gor ${r.method.name} request on ${r.uri} with correct header ${r.headers("header1")} and payload '$body'")
          UIO(Response.plain("valid-token"))
        } else {
          log.info(s"gor ${r.method.name} request on ${r.uri} that is invalid, failing the fiber")
          IO.fail(Forbidden("No access"))
        }
    } yield out

    Server.builder(new InetSocketAddress("0.0.0.0", port)).handleSome {
      case r if r.uri.getPath == "/random"                                => randomJson(r).provide(s"fake-random-$port")
      case r if r.uri.getPath == "/paginated"                             => paginatedJson(r).provide(s"fake-paginated-$port")
      case r if r.uri.getPath == "/auth-request-form" && r.method == POST => authChallenge(r).provide(s"fake-auth-request-form-$port")
      case r if r.uri.getPath == "auth-token" =>
        if (allowLogin && r.headers.get("Authorization").contains("Bearer valid-token"))
          randomJson(r).provide(s"fake-auth-token-$port")
        else
          IO.fail(Forbidden("No access"))
      case _ => IO.fail(NotFound("No such resource"))
    }
  }

  def withServer[R](
      body: (Int, Ref[ServerLog]) => RIO[R, TestResult]
  ): RIO[R with Blocking with Clock with Random, TestResult] =
    for {
      ref        <- Ref.make(ServerLog(None))
      randomPort <- random.nextIntBetween(10000, 50000)
      out        <- serverResource(ref, randomPort).serve.use(_ => body(randomPort, ref))
    } yield out
}
