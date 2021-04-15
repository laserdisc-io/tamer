package tamer.rest

import _root_.zio.test.environment.TestEnvironment
import io.circe.Codec
import sttp.model.Uri
import tamer.TamerError
import tamer.config.KafkaConfig
import tamer.kafka.embedded.KafkaTest
import zio.duration.durationInt
import zio.test.Assertion._
import zio.test.TestAspect.timeout
import zio.test.{assertM, _}
import zio.{Chunk, Has, Queue, Ref, Task, UIO, ZEnv, ZIO, ZLayer}

import scala.annotation.unused

object RestSpec extends DefaultRunnableSpec {
  import Fixtures._
  import sttp.client3._
  import sttp.client3.httpclient.zio._

  class JobFixtures(port: Int) {

    val fullLayer: ZLayer[ZEnv, Throwable, SttpClient] =
      HttpClientZioBackend.layer()

    private val qb: RestQueryBuilder[State] = new RestQueryBuilder[State] {
      override val queryId: Int = 0

      override def query(state: State): Uri = uri"http://localhost:$port/random"
    }

    private val qbAuth: RestQueryBuilder[State] = new RestQueryBuilder[State] {
      override val queryId: Int = 0

      override def query(state: State) = uri"http://localhost:$port/auth-token"
    }

    val conf: RestConfiguration[Any, Key, Value, State] = RestConfiguration.apply(qb)(StaticFixtures.decoder)(StaticFixtures.transitions)

    val confAuth: RestConfiguration[ZEnv with SttpClient with KafkaConfig with Has[Ref[KafkaLog]], Key, Value, State] =
      new RestConfiguration(
        qbAuth,
        StaticFixtures.decoder,
        StaticFixtures.transitions
      )

    val outLayer: ZLayer[Any, Nothing, Has[Ref[KafkaLog]]] = Ref.make(KafkaLog(0)).toLayer

    val job = new TamerRestJob[ZEnv with SttpClient with KafkaConfig with Has[Ref[KafkaLog]], Key, Value, State](conf) {
      override protected def next(
          currentState: State,
          q: Queue[Chunk[(Key, Value)]]
      ): ZIO[ZEnv with SttpClient with KafkaConfig with Has[Ref[KafkaLog]], TamerError, State] =
        for {
          next <- super.next(currentState, q)
          log  <- ZIO.service[Ref[KafkaLog]]
          _    <- log.update(l => l.copy(l.count + 1))
        } yield next
    }

    val jobAuth = new TamerRestJob[ZEnv with SttpClient with KafkaConfig with Has[Ref[KafkaLog]], Key, Value, State](confAuth) {
      override protected def next(
          currentState: State,
          q: Queue[Chunk[(Key, Value)]]
      ): ZIO[ZEnv with SttpClient with KafkaConfig with Has[Ref[KafkaLog]], TamerError, State] =
        for {
          next <- super.next(currentState, q)
          log  <- ZIO.service[Ref[KafkaLog]]
          _    <- log.update(l => l.copy(l.count + 1))
        } yield next
    }

    val tamerLayer: ZLayer[ZEnv, Throwable, ZEnv with SttpClient] =
      ZLayer.requires[ZEnv] ++ fullLayer
  }

  case class KafkaLog(count: Int)

  object StaticFixtures {
    val decoder: String => Task[DecodedPage[Value, State]] = DecodedPage.fromString { v =>
      (for {
         p <- ZIO.fromEither(io.circe.parser.parse(v))
         out <- ZIO.fromEither(implicitly[Codec[Value]].decodeJson(p))
      } yield List(out)).catchAll(e => ZIO.fail(new RuntimeException(s"Decoder failed!\n$e")))
    }

    val transitions = RestConfiguration.State(State(0))(s => UIO(s.copy(count = s.count + 1)), (_, v: Value) => Key(v.time))

    val kafkaConfigLayer: ZLayer[ZEnv, Throwable, KafkaConfig] = KafkaTest.embeddedKafkaTest >>> KafkaTest.embeddedKafkaConfig
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

    val io: ZIO[ZEnv with SttpClient with KafkaConfig with Has[Ref[KafkaLog]], Throwable, TestResult] = for {
      _ <- f.job.fetch().fork
      _ <- (ZIO.effect(println("Awaiting a request to our test server")) *> ZIO.sleep(500.millis))
        .repeatUntilM(_ => serverLog.get.map(_.lastRequestTimestamp.isDefined))
      output <- ZIO.service[Ref[KafkaLog]]
      _ <- (ZIO.effect(println("Awaiting state change")) *> ZIO.sleep(500.millis))
        .repeatUntilM(_ => output.get.map(_.count > 0))
    } yield assertCompletes

    io.provideSomeLayer[ZEnv with KafkaConfig](f.tamerLayer ++ f.outLayer)
  }

  private def testRestFlowAuth(port: Int, serverLog: Ref[ServerLog]) = {
    val f = new JobFixtures(port)

    val io: ZIO[ZEnv with SttpClient with KafkaConfig with Has[Ref[KafkaLog]], Throwable, TestResult] = for {
      _ <- f.jobAuth.fetch().fork
      _ <- (ZIO.effect(println("Awaiting a request to our test server")) *> ZIO.sleep(500.millis))
        .repeatUntilM(_ => serverLog.get.map(_.lastRequestTimestamp.isDefined))
      output <- ZIO.service[Ref[KafkaLog]]
      _ <- (ZIO.effect(println("Awaiting state change")) *> ZIO.sleep(500.millis))
        .repeatUntilM(_ => output.get.map(_.count > 0))
    } yield assertCompletes

    io.provideSomeLayer[ZEnv with KafkaConfig](f.tamerLayer ++ f.outLayer)
  }

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite("RestSpec")(
      testM("Should run a test with provisioned http4s")(withServer(testHttp4sStartup)),
      testM("Should support e2e rest flow")(withServer(testRestFlow)) @@ timeout(30.seconds),
      testM("Should support auth flow")(withServer(testRestFlowAuth)) @@ timeout(30.seconds)
    )
      .provideSomeLayerShared[ZEnv with TestEnvironment](StaticFixtures.kafkaConfigLayer.mapError(e => TestFailure.die(e)))
      .provideCustomLayer(ZEnv.live)
}
