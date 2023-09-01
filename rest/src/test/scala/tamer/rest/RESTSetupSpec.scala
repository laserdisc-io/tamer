package tamer
package rest

import com.comcast.ip4s.Port
import io.circe.parser
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.client3._
import sttp.client3.httpclient.zio._
import vulcan.Codec
import vulcan.generic._
import zio._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

import scala.annotation.unused

object RESTSetupSpec extends ZIOSpecDefault with HttpServerSupport {
  implicit final val stateKeyVulcanCodec: Codec[Tamer.StateKey] = Codec.derive[Tamer.StateKey]

  case class Log(count: Int)
  object Log {
    val layer: ULayer[Ref[Log]] = ZLayer(Ref.make(Log(0)))
  }

  final case class Fixtures(port: Port) {
    private[this] def queryFor(@unused state: State): Request[Either[String, String], Any] =
      basicRequest.get(uri"http://localhost:${port.value}/random").readTimeout(20.seconds.asScala)

    private[this] val decoder: String => Task[DecodedPage[Value, State]] = DecodedPage.fromString { v =>
      ZIO.fromEither(parser.decode[Value](v)).map(List(_)).catchAll(e => ZIO.fail(new RuntimeException(s"Decoder failed!\n$e")))
    }

    val rest = RESTSetup(State(0))(
      queryFor,
      decoder,
      (_: State, v: Value) => Key(v.time),
      (_: DecodedPage[Value, State], s: State) => ZIO.service[Ref[Log]].flatMap(_.update(l => Log(l.count + 1))) *> ZIO.succeed(State(s.count + 1))
    ).run
  }

  private def testHttpStartup(port: Port, @unused serverLog: Ref[ServerLog]): Task[TestResult] = for {
    cb       <- HttpClientZioBackend()
    resp     <- cb.send(basicRequest.get(uri"http://localhost:${port.value}/random"))
    respBody <- ZIO.fromEither(resp.body.left.map(e => new RuntimeException(s"Unexpected result: $e")))
    out      <- assert(respBody)(containsString("uuid"))
    _        <- log4sFromName.provideEnvironment(ZEnvironment("test-http-startup")).flatMap(_.info(s"conclusion is: $out"))
  } yield out

  private def testRestFlow(port: Port, sl: Ref[ServerLog]) = {
    val test = for {
      log    <- log4sFromName.provideEnvironment(ZEnvironment("test-rest-flow"))
      fiber  <- Fixtures(port).rest.fork
      _      <- (log.info("awaiting a request to our test server") *> ZIO.sleep(500.millis)).repeatUntilZIO(_ => sl.get.map(_.lastRequest.isDefined))
      output <- ZIO.service[Ref[Log]]
      _      <- (log.info("awaiting state change") *> ZIO.sleep(500.millis)).repeatUntilZIO(_ => output.get.map(_.count > 0))
    } yield assertCompletes
    test.provideLayer(Log.layer ++ restLive() ++ FakeKafka.embeddedKafkaConfigLayer)
  }

  override final val spec = suite("RESTSetupSpec")(
    test("Should run a test with provisioned http4s")(withServer(testHttpStartup)),
    test("Should support e2e rest flow")(withServer(testRestFlow)).mapError(TestFailure.die) @@ timeout(1.minute)
  ) @@ nondeterministic @@ withLiveClock
}
