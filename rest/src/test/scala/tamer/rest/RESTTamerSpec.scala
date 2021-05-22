package tamer
package rest

import _root_.zio.test.environment.TestEnvironment
import io.circe.Codec
import tamer.TamerError
import tamer.kafka.KafkaConfig
import tamer.kafka.FakeKafka
import tamer.rest.LocalSecretCache.LocalSecretCache
import zio.duration.durationInt
import zio.test.Assertion._
import zio.test.TestAspect.timeout
import zio.test.{assertM, _}
import zio.{Chunk, Has, Queue, RIO, RLayer, Ref, Task, UIO, ZEnv, ZIO, ZLayer}

import java.util.concurrent.TimeUnit
import scala.annotation.{nowarn, unused}
import scala.concurrent.duration.Duration

object RESTTamerSpec extends DefaultRunnableSpec {
  import Fixtures._
  import sttp.client3._
  import sttp.client3.httpclient.zio._

  class JobFixtures(port: Int) {

    val sttpLayer = HttpClientZioBackend.layer()

    private val qb: QueryBuilder[Any, State] = new QueryBuilder[Any, State] {
      override val queryId: Int = 0

      override def query(state: State): Request[Either[String, String], Any] =
        basicRequest.get(uri"http://localhost:$port/random").readTimeout(Duration(20, TimeUnit.SECONDS))
    }

    val conf: RESTSetup[Any, Key, Value, State] = new RESTSetup(qb, StaticFixtures.decoder)(StaticFixtures.transitions)

    val outLayer: ZLayer[Any, Nothing, Has[Ref[KafkaLog]]] = Ref.make(KafkaLog(0)).toLayer

    val job = new RESTTamer[ZEnv with SttpClient with Has[KafkaConfig] with Has[Ref[KafkaLog]] with LocalSecretCache, Key, Value, State](conf) {
      override protected def next(
          currentState: State,
          q: Queue[Chunk[(Key, Value)]]
      ): ZIO[ZEnv with SttpClient with Has[KafkaConfig] with Has[Ref[KafkaLog]] with LocalSecretCache, TamerError, State] =
        for {
          next <- super.next(currentState, q)
          log  <- ZIO.service[Ref[KafkaLog]]
          _    <- log.update(l => l.copy(l.count + 1))
        } yield next
    }

    val tamerLayer: RLayer[ZEnv, ZEnv with SttpClient with Has[Ref[Option[String]]]] =
      ZLayer.requires[ZEnv] ++ sttpLayer ++ Ref.make(Option.empty[String]).toLayer
  }

  case class KafkaLog(count: Int)

  object StaticFixtures {
    val decoder: String => Task[DecodedPage[Value, State]] = DecodedPage.fromString { v =>
      (for {
        p   <- ZIO.fromEither(io.circe.parser.parse(v))
        out <- ZIO.fromEither(implicitly[Codec[Value]].decodeJson(p))
      } yield List(out)).catchAll(e => ZIO.fail(new RuntimeException(s"Decoder failed!\n$e")))
    }

    @nowarn
    val transitions = new RESTSetup.State(State(0))((_, v: Value) => Key(v.time))((_, s) => UIO(s.copy(count = s.count + 1)))

    val kafkaConfigLayer: ZLayer[ZEnv, Throwable, Has[KafkaConfig]] = FakeKafka.embedded >>> FakeKafka.embeddedKafkaConfig
  }

  private def testHttp4sStartup(port: Int, @unused serverLog: Ref[ServerLog]) =
    for {
      cb <- HttpClientZioBackend()
      req = basicRequest.get(uri"http://localhost:$port/random")
      resp     <- cb.send(req)
      respBody <- Task.fromEither(resp.body.left.map(e => new RuntimeException(s"Unexpected result: $e")))
      out      <- assertM(Task.succeed(respBody))(containsString("uuid"))
    } yield out

  private def testRestFlow(port: Int, serverLog: Ref[ServerLog]) = {
    val f = new JobFixtures(port)

    val io: RIO[ZEnv with SttpClient with Has[KafkaConfig] with Has[Ref[KafkaLog]] with LocalSecretCache, TestResult] = for {
      _ <- f.job.run.fork
      _ <- (ZIO.effect(println("Awaiting a request to our test server")) *> ZIO.sleep(500.millis))
        .repeatUntilM(_ => serverLog.get.map(_.lastRequestTimestamp.isDefined))
      output <- ZIO.service[Ref[KafkaLog]]
      _ <- (ZIO.effect(println("Awaiting state change")) *> ZIO.sleep(500.millis))
        .repeatUntilM(_ => output.get.map(_.count > 0))
    } yield assertCompletes

    io.provideSomeLayer[ZEnv with Has[KafkaConfig]](f.tamerLayer ++ f.outLayer)
  }

  override final val spec = suite("RESTTamerSpec")(
    testM("Should run a test with provisioned http4s")(withServer(testHttp4sStartup)),
    testM("Should support e2e rest flow")(withServer(testRestFlow)) @@ timeout(30.seconds)
  )
    .provideSomeLayerShared[ZEnv with TestEnvironment](StaticFixtures.kafkaConfigLayer.mapError(e => TestFailure.die(e)))
    .provideCustomLayer(ZEnv.live)
}
