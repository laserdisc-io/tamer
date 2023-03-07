package tamer
package rest

import io.circe.parser
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.client3._
import sttp.client3.httpclient.zio._
import zio._
import zio.Clock

import zio.test.Assertion._
import zio.test.TestAspect.timeout
import zio.test.{assertM, _}
import zio.test.environment.TestEnvironment

import scala.annotation.unused
import zio.Random
import zio.test.ZIOSpecDefault

object RESTSetupSpec extends ZIOSpecDefault with UzHttpServerSupport {
  case class Log(count: Int)
  object Log {
    val layer: ULayer[Ref[Log]] = Ref.make(Log(0)).toLayer
  }

  final case class Fixtures(port: Int) {
    private[this] def queryFor(@unused state: State): Request[Either[String, String], Any] =
      basicRequest.get(uri"http://localhost:$port/random").readTimeout(20.seconds.asScala)

    private[this] val decoder: String => Task[DecodedPage[Value, State]] = DecodedPage.fromString { v =>
      ZIO.fromEither(parser.decode[Value](v)).map(List(_)).catchAll(e => ZIO.fail(new RuntimeException(s"Decoder failed!\n$e")))
    }

    val rest = RESTSetup(State(0))(
      queryFor,
      decoder,
      (_: State, v: Value) => Key(v.time),
      (_: DecodedPage[Value, State], s: State) =>
        ZIO.service[Ref[Log]].flatMap(_.update(l => l.copy(l.count + 1))) *> URIO(s.copy(count = s.count + 1))
    ).run
  }

  private def testUzHttpStartup(port: Int, @unused serverLog: Ref[ServerLog]) = for {
    cb       <- HttpClientZioBackend()
    resp     <- cb.send(basicRequest.get(uri"http://localhost:$port/random"))
    respBody <- ZIO.fromEither(resp.body.left.map(e => new RuntimeException(s"Unexpected result: $e")))
    out      <- assertM(ZIO.succeed(respBody))(containsString("uuid"))
  } yield out

  private def testRestFlow(port: Int, sl: Ref[ServerLog]) = for {
    log    <- log4sFromName.provideService("test-rest-flow")
    _      <- Fixtures(port).rest.fork
    _      <- (log.info("awaiting a request to our test server") *> ZIO.sleep(500.millis)).repeatUntilZIO(_ => sl.get.map(_.lastRequest.isDefined))
    output <- ZIO.service[Ref[Log]]
    _      <- (log.info("awaiting state change") *> ZIO.sleep(500.millis)).repeatUntilZIO(_ => output.get.map(_.count > 0))
  } yield assertCompletes

  override final val spec = suite("RESTSetupSpec")(
    test("Should run a test with provisioned uzHttp")(withServer(testUzHttpStartup)),
    test("Should support e2e rest flow")(withServer(testRestFlow)) @@ timeout(30.seconds)
  ).provideSomeLayerShared[TestEnvironment](
    (Log.layer ++ restLive() ++ FakeKafka.embeddedKafkaConfigLayer).mapError(TestFailure.die)
  ).updateService[Clock.Service](_ => Clock.Service.live)
    .updateService[Random](_ => Random.Service.live)
}
