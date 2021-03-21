package tamer.rest

import io.circe.generic.semiauto.deriveCodec
import tamer.{AvroCodec, HashableState}
import zio.interop.catz._
import zio.interop.catz.implicits._
import zio.test._
import zio.{Task, ZIO, _}

import java.util.UUID
import scala.util.Random

object Fixtures {
  import org.http4s._
  import org.http4s.dsl.Http4sDsl
  import org.http4s.implicits._
  import org.http4s.server.Router
  import org.http4s.server.blaze._

  case class Key(time: Long)
  object Key {
    implicit val codec = AvroCodec.codec[Key]
  }
  case class Value(time: Long, uuid: UUID)
  object Value {
    implicit val codec      = AvroCodec.codec[Value]
    implicit val circeCodec = deriveCodec[Value]
  }
  case class State(i: Int)
  object State {
    implicit val codec = AvroCodec.codec[State]
    implicit val hash = new HashableState[State] {
      override def stateHash(s: State): Int = s.i
    }
  }

  private val dsl: Http4sDsl[Task] = new Http4sDsl[Task] {}

  private val testService: HttpRoutes[Task] = {
    import dsl._

    HttpRoutes.of[Task] { case r @ GET -> Root / "random" =>
      for {
        now <- Task.succeed(System.nanoTime())
        uid <- Task.succeed(UUID.randomUUID())
        _   <- Task.succeed(println(s"Got request $r"))
        out <- Ok(s"""{"time": $now, "uuid": "$uid"}""")
      } yield out
    }
  }
  private val httpApp = Router("/" -> testService).orNotFound

  def serverResource =
    for {
      port <- ZIO.succeed(Random.nextInt(20000) + 10000)
      serverBuilder = ZIO.runtime.map { implicit r: Runtime[Any] =>
        BlazeServerBuilder[Task](scala.concurrent.ExecutionContext.Implicits.global).bindHttp(port, "localhost").withHttpApp(httpApp)
      }
      sb <- serverBuilder
    } yield sb.resource.map(s => (s, port)).toManagedZIO

  def withServer[R](body: Int => ZIO[R, Throwable, TestResult]): ZIO[R, Throwable, TestResult] =
    for {
      sp  <- serverResource
      out <- sp.use { case (_, p) => body(p) }
    } yield out
}
