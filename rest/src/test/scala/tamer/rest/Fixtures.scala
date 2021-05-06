package tamer.rest

import io.circe.generic.semiauto.deriveCodec
import sttp.client3.UriContext
import tamer.{AvroCodec, HashableState}
import uzhttp.HTTPError.{Forbidden, NotFound}
import uzhttp.Request.Method.POST
import uzhttp.server.Server
import uzhttp.{Request, Response}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.random.Random
import zio.stream.ZTransducer
import zio.test._
import zio.{Task, _}

import java.net.InetSocketAddress
import java.util.UUID

object Fixtures {

  var allowLogin: Boolean = true

  case class Key(time: Long)
  object Key {
    implicit val codec = AvroCodec.codec[Key]
  }
  case class Value(time: Long, uuid: UUID)
  object Value {
    implicit val codec      = AvroCodec.codec[Value]
    implicit val circeCodec = deriveCodec[Value]
  }
  case class State(count: Int)
  object State {
    implicit val codec = AvroCodec.codec[State]
    implicit val hash = new HashableState[State] {
      override def stateHash(s: State): Int = s.count
    }
  }

  case class ServerLog(lastRequestTimestamp: Option[Long])

  def serverResource(gotRequest: Ref[ServerLog], port: Int) = {
    val jsonHeaders = List("content-type" -> "application/json")

    def randomJson(r: Request) = for {
      now <- Task.succeed(System.nanoTime())
      uid <- Task.succeed(UUID.randomUUID())
      _   <- Task.succeed(println(s"Got request ${r.method.name} ${r.uri.toString}"))
      _   <- gotRequest.update(l => l.copy(lastRequestTimestamp = Some(now)))
      out <- UIO(Response.plain(s"""{"time": $now, "uuid": "$uid"}""", headers = jsonHeaders))
    } yield out

    def paginatedJson(r: Request) = for {
      page <- Task.succeed(uri"${r.uri.toString}".paramsMap("page").toLong)
      uid  <- Task.succeed(UUID.randomUUID())
      _    <- Task.succeed(println(s"Got request ${r.method.name} ${r.uri.toString}"))
      _    <- gotRequest.update(l => l.copy(lastRequestTimestamp = Some(page)))
      out  <- UIO(Response.plain(s"""{"time": $page, "uuid": "$uid"}""", headers = jsonHeaders))
    } yield out

    Server.builder(new InetSocketAddress("0.0.0.0", port)).handleSome {
      case r if r.uri.getPath == "/random"    => randomJson(r)
      case r if r.uri.getPath == "/paginated" => paginatedJson(r)
      case r if r.uri.getPath == "/auth-request-form" && r.method == POST =>
        for {
          body <- r.body.map(_.transduce(ZTransducer.utf8Decode).runCollect.map(_.mkString)).getOrElse(UIO(""))
          out <-
            if (r.headers.get("header1").contains("value1") && body == "valid body")
              UIO(Response.plain("valid-token"))
            else
              IO.fail(Forbidden("No access"))
        } yield out
      case r if r.uri.getPath == "auth-token" =>
        for {
          out <- if (allowLogin && r.headers.get("Authorization").contains("Bearer valid-token")) randomJson(r) else IO.fail(Forbidden("No access"))
        } yield out
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
