package tamer
package rest

import log.effect.zio.ZioLogWriter.log4sFromName
import zio._
import zio.duration._
import zio.test.Assertion._
import zio.test.TestAspect.timeout
import zio.test.{assertM, _}
import zio.test.environment.TestEnvironment

import scala.annotation.unused

object RESTTamerSpec extends DefaultRunnableSpec {
  import Fixtures._
  import sttp.client3._
  import sttp.client3.httpclient.zio._

  class JobFixtures(port: Int) {
    private val qb: QueryBuilder[Any, State] = new QueryBuilder[Any, State] {
      override val queryId: Int = 0

      override def query(state: State): Request[Either[String, String], Any] =
        basicRequest.get(uri"http://localhost:$port/random").readTimeout(20.seconds.asScala)
    }

    val decoder: String => Task[DecodedPage[Value, State]] = DecodedPage.fromString { v =>
      ZIO.fromEither(io.circe.parser.decode[Value](v)).map(List(_)).catchAll(e => ZIO.fail(new RuntimeException(s"Decoder failed!\n$e")))
    }

    val rest = RESTSetup(
      qb,
      decoder,
      State(0),
      (_: State, v: Value) => Key(v.time),
      (_: DecodedPage[Value, State], s: State) =>
        URIO.service[Ref[KafkaLog]].flatMap(_.update(l => l.copy(l.count + 1))) *> URIO(s.copy(count = s.count + 1))
    ).runLive
  }

  case class KafkaLog(count: Int)

  private def testUzHttpStartup(port: Int, @unused serverLog: Ref[ServerLog]) = for {
    cb       <- HttpClientZioBackend()
    resp     <- cb.send(basicRequest.get(uri"http://localhost:$port/random"))
    respBody <- Task.fromEither(resp.body.left.map(e => new RuntimeException(s"Unexpected result: $e")))
    out      <- assertM(Task.succeed(respBody))(containsString("uuid"))
  } yield out

  private def testRestFlow(port: Int, serverLog: Ref[ServerLog]) = {
    val f = new JobFixtures(port)

    for {
      log <- log4sFromName.provide("test-rest-flow")
      _   <- f.rest.fork
      _ <- (log.info("awaiting a request to our test server") *> ZIO.sleep(500.millis)).repeatUntilM(_ =>
        serverLog.get.map(_.lastRequestTimestamp.isDefined)
      )
      output <- ZIO.service[Ref[KafkaLog]]
      _      <- (log.info("Awaiting state change") *> ZIO.sleep(500.millis)).repeatUntilM(_ => output.get.map(_.count > 0))
    } yield assertCompletes
  }

  override final val spec = suite("RESTTamerSpec")(
    testM("Should run a test with provisioned uzHttp")(withServer(testUzHttpStartup)),
    testM("Should support e2e rest flow")(withServer(testRestFlow)) @@ timeout(30.seconds)
  ).provideSomeLayerShared[ZEnv with TestEnvironment] {
    (Ref.make(KafkaLog(0)).toLayer ++ restLive() ++ (FakeKafka.embedded >>> FakeKafka.embeddedKafkaConfig)).mapError(TestFailure.die)
  }.provideCustomLayer(ZEnv.live)
}
