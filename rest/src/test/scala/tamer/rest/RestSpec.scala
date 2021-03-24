package tamer.rest

import _root_.zio.test.environment.TestEnvironment
import io.circe.Codec
import sttp.capabilities.zio.ZioStreams
import tamer.config.KafkaConfig
import tamer.kafka.Kafka
import tamer.kafka.embedded.KafkaTest
import tamer.rest.RestSpec.StaticFixtures.KafkaLog
import tamer.{SourceConfiguration, TamerError}
import zio.duration.durationInt
import zio.stream.ZTransducer
import zio.test.Assertion._
import zio.test.TestAspect.timeout
import zio.test.{assertM, _}
import zio.{Chunk, Has, Queue, Ref, Task, ZEnv, ZIO, ZLayer, stream}

import java.util.concurrent.TimeUnit
import scala.annotation.unused
import scala.concurrent.duration.Duration

object RestSpec extends DefaultRunnableSpec {
  import Fixtures._
  import sttp.client3._
  import sttp.client3.httpclient.zio._

  class JobFixtures(port: Int) {

    val fullLayer: ZLayer[ZEnv, Throwable, SttpClient] =
      HttpClientZioBackend.layer()

    private val qb: RestQueryBuilder[State] = new RestQueryBuilder[State] {
      override val queryId: Int = 0

      override def query(state: State): RequestT[Identity, Either[String, stream.Stream[Throwable, Byte]], ZioStreams] =
        basicRequest
          .get(uri"http://localhost:$port/random")
          .response(asStreamUnsafe(ZioStreams))
          .readTimeout(Duration.create(20, TimeUnit.SECONDS))
    }

    val transducer: ZTransducer[ZEnv with SttpClient, TamerError, Byte, Value] = {
      ZTransducer.utf8Decode >>> ZTransducer.fromFunctionM { v: String =>
        (for {
          p   <- ZIO.fromEither(io.circe.parser.parse(v))
          out <- ZIO.fromEither(implicitly[Codec[Value]].decodeJson(p))

        } yield out).catchAll(e => ZIO.fail(new TamerError(s"Decoder failed!", e)))
      }
    }

    val transitions: RestConfiguration.State[Key, Value, State] =
      RestConfiguration.State[Key, Value, State](State(0), s => ZIO.succeed(s), (_, v) => Key(v.time))

    val conf: RestConfiguration[ZEnv with SttpClient with KafkaConfig, Key, Value, State] =
      new RestConfiguration[ZEnv with SttpClient with KafkaConfig, Key, Value, State](
        qb,
        transducer,
        transitions
      )

    val job = new TamerRestJob[ZEnv with SttpClient with KafkaConfig, Key, Value, State](conf)

    val tamerLayer: ZLayer[ZEnv, Throwable, ZEnv with SttpClient] =
      ZLayer.requires[ZEnv] ++ fullLayer
  }


  object StaticFixtures {
    case class KafkaLog(count: Int, last: Option[Chunk[(Key, Value)]])

    val output = Ref.make(KafkaLog(0, None))

    val stateTransitionFunction: (State, Queue[Chunk[(Key, Value)]]) => ZIO[Has[Ref[KafkaLog]], TamerError, State] = { case (s, kv) =>
      for {
        log     <- ZIO.service[Ref[KafkaLog]]
        content <- kv.takeAll
        _       <- ZIO.succeed(log.update(l => l.copy(l.count + 1, last = content.lastOption)))
        _       <- kv.offerAll(content)
      } yield {
        System.exit(0)
        s.copy(s.i + 1)
      }

    }

    val kafkaConfigLayer: ZLayer[ZEnv, Throwable, KafkaConfig] = KafkaTest.embeddedKafkaTest >>> KafkaTest.embeddedKafkaConfig
    val kafkaLayer: ZLayer[ZEnv, Throwable, KafkaConfig with Kafka with Has[Ref[KafkaLog]]] = {
      kafkaConfigLayer ++ output.toLayer ++ ((ZLayer.requires[ZEnv] ++ kafkaConfigLayer ++ output.toLayer) >>> Kafka.live(
        SourceConfiguration(SourceConfiguration.SourceSerde[Key, Value, State](), State(0), 0),
        stateTransitionFunction
      ))
    }
  }
  private def testHttp4sStartup(port: Int, @unused gotRequest: Ref[ServerLog]) =
    for {
      _  <- Task.succeed(gotRequest)
      cb <- HttpClientZioBackend()
      req = basicRequest.get(uri"http://localhost:$port/random")
      resp     <- cb.send(req)
      respBody <- Task.fromEither(resp.body.left.map(e => new RuntimeException(s"Unexpected result: $e")))
      _        <- Task.succeed(println(respBody))
      out      <- assertM(Task.succeed(respBody))(containsString("uuid"))
    } yield out

  private def testRestFlow(port: Int, gotRequest: Ref[ServerLog]) = {
    val f = new JobFixtures(port)

    val io: ZIO[ZEnv with SttpClient with KafkaConfig with Kafka with Has[Ref[KafkaLog]], Throwable, TestResult] = for {
      _ <- f.job.fetch().fork
      _ <- (ZIO.effect(println("Awaiting a request to our test server")) *> ZIO.sleep(500.millis))
        .repeatUntilM(_ => gotRequest.get.map(_.lastRequestTimestamp.isDefined))
      output <- ZIO.service[Ref[KafkaLog]]
      _ <- (ZIO.effect(println("Awaiting kafka state change")) *> ZIO.sleep(500.millis))
        .repeatUntilM(_ => output.get.map(_.last.isDefined))
    } yield assertCompletes

    io.provideSomeLayer[ZEnv with KafkaConfig with Kafka with Has[Ref[KafkaLog]]](f.tamerLayer)
  }

  override def spec: Spec[TestEnvironment, TestFailure[Throwable], TestSuccess] =
    suite("RestSpec")(
      testM("Should run a test with provisioned http4s")(withServer(testHttp4sStartup)),
      testM("Should support e2e rest flow")(withServer(testRestFlow)) @@ timeout(30.seconds)
    )
      .provideSomeLayerShared[ZEnv with TestEnvironment](StaticFixtures.kafkaLayer.mapError(e => TestFailure.die(e)))
      .provideCustomLayer(ZEnv.live)
}
