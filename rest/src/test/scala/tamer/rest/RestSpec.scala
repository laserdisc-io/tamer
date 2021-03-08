package tamer.rest

import zio.interop.catz._
import zio.interop.catz.implicits._
import zio.test.Assertion._
import zio.test.environment.TestEnvironment
import zio.test.{assertM, _}
import zio.{Task, ZIO, _}

import java.util.UUID

object Fixtures {
  import org.http4s._
  import org.http4s.dsl.Http4sDsl
  import org.http4s.implicits._
  import org.http4s.server.Router
  import org.http4s.server.blaze._

  val port = 8081

  private val dsl: Http4sDsl[Task] = new Http4sDsl[Task] {}

  private val testService: HttpRoutes[Task] = {
    import dsl._

    HttpRoutes.of[Task] { case GET -> Root / "random" =>
      for {
        now <- Task.succeed(System.nanoTime())
        uid <- Task.succeed(UUID.randomUUID())
        out <- Ok(s"""{"time": $now, "uuid": "$uid"}""")
      } yield out
    }
  }
  private val httpApp = Router("/" -> testService).orNotFound

  private val serverBuilder: ZIO[Any, Nothing, BlazeServerBuilder[Task]] = ZIO.runtime.map { implicit r: Runtime[Any] =>
    BlazeServerBuilder[Task](scala.concurrent.ExecutionContext.Implicits.global).bindHttp(port, "localhost").withHttpApp(httpApp)
  }

  def withServer(body: ZIO[Any, Throwable, TestResult]): ZIO[TestEnvironment, Throwable, TestResult] =
    for {
      sb  <- serverBuilder
      out <- sb.resource.use(_ => body)
    } yield out
}

object DateParsingSpec extends DefaultRunnableSpec {
  import Fixtures._
  import sttp.client3._
  import sttp.client3.httpclient.zio._

  private def testHttp4sStartup =
    for {
      cb <- HttpClientZioBackend()
      req = basicRequest.get(uri"http://localhost:$port/random")
      resp     <- cb.send(req)
      respBody <- Task.fromEither(resp.body.left.map(e => new RuntimeException(s"Unexpected result: $e")))
      _        <- Task.succeed(println(respBody))
      out      <- assertM(Task.succeed(respBody))(containsString("uuid"))
    } yield out

  override def spec: ZSpec[TestEnvironment, Any] = suite("RestSpec")(
    testM("Should run a test with provisioned http4s")(withServer(testHttp4sStartup))
  )
}
