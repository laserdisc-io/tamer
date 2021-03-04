package tamer.example

import eu.timepit.refined._
import eu.timepit.refined.auto._
import eu.timepit.refined.boolean.{And, Or}
import eu.timepit.refined.collection.{Forall, NonEmpty}
import eu.timepit.refined.string.{IPv4, Uri}
import software.amazon.awssdk.regions.Region
import tamer.config.Config.Kafka
import tamer.config.{Config, KafkaConfig}
import tamer.s3.TamerS3.TamerS3Impl
import tamer.s3.{Keys, KeysR, S3Configuration}
import tamer.{AvroCodec, TamerError}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.s3.{InvalidCredentials, S3, S3Credentials}
import zio.stream.{Transducer, ZTransducer}
import zio.{ExitCode, Has, Layer, Queue, UIO, URIO, ZIO}

import java.net.URI
import scala.concurrent.duration.{FiniteDuration => ScalaFD}
import scala.util.hashing.MurmurHash3.stringHash

final case class Line(str: String)
object Line {
  implicit val codec = AvroCodec.codec[Line]
}

final case class LastProcessedNumber(number: Long)

object LastProcessedNumber {
  implicit val codec = AvroCodec.codec[LastProcessedNumber]
}

object S3Generalized extends zio.App {
  lazy val mkS3KafkaLayer: ZIO[Blocking, InvalidCredentials, Layer[RuntimeException, S3 with KafkaConfig]] =
    S3Credentials.fromAll.map { s3Credentials =>
      val kafkaState: Config.KafkaState = Config.KafkaState("state-topic", "groupid", "clientid")
      val kafkaSink: Config.KafkaSink   = Config.KafkaSink("sink-topic")
      val hostList                      = refineV[NonEmpty And Forall[IPv4 Or Uri]](List("localhost:9092"))
      val kafkaConfigLayer: Layer[String, Has[Kafka]] =
        ZIO.fromEither(hostList.map(hl => Config.Kafka(hl, "http://localhost:8081", ScalaFD(10, "seconds"), 50, kafkaSink, kafkaState))).toLayer
      zio.s3.live(Region.AF_SOUTH_1, s3Credentials, Some(new URI("http://localhost:9000"))) ++ kafkaConfigLayer.mapError(e => TamerError(e))
    }

  private val myTransducer: Transducer[Nothing, Byte, Line] =
    ZTransducer.utf8Decode >>> ZTransducer.splitLines.map(Line.apply)

  private final def getNextNumber(
      keysR: KeysR,
      afterwards: LastProcessedNumber,
      prefix: String
  ): ZIO[Any, Nothing, Option[Long]] = keysR.get.map { keys =>
    val sortedFileNumbers = keys
      .map(key => key.stripPrefix(prefix).toLong)
      .filter(number => afterwards.number < number)

    if (sortedFileNumbers.isEmpty) None else Some(sortedFileNumbers.min)
  }

  private final def getNextState(prefix: String)(
      keysR: KeysR,
      afterwards: LastProcessedNumber,
      keysChangedToken: Queue[Unit]
  ): UIO[LastProcessedNumber] = {
    val retryAfterWaitingForKeyListChange =
      keysChangedToken.take *> getNextState(prefix)(keysR, afterwards, keysChangedToken)
    getNextNumber(keysR, afterwards, prefix)
      .flatMap {
        case Some(number) if number > afterwards.number => UIO(LastProcessedNumber(number))
        case _                                          => retryAfterWaitingForKeyListChange
      }
  }

  private final def selectObjectForInstant(lastProcessedNumber: LastProcessedNumber): Option[String] =
    Some(s"myFolder2/myPrefix${lastProcessedNumber.number}")

  private val setup: S3Configuration[Any, LastProcessedNumber, Line, LastProcessedNumber] = S3Configuration(
    bucketName = "myBucket",
    prefix = "myFolder2/myPrefix",
    tamerStateKafkaRecordKey = stringHash("myBucket") + stringHash("myFolder2/myPrefix") + 0,
    transducer = myTransducer,
    parallelism = 1,
    S3Configuration.S3PollingTimings(
      minimumIntervalForBucketFetch = 1.second,
      maximumIntervalForBucketFetch = 1.minute
    ),
    S3Configuration.State(
      initialState = LastProcessedNumber(0),
      getNextState = getNextState("myFolder2/myPrefix"),
      deriveKafkaRecordKey = (l: LastProcessedNumber, _: Line) => l,
      selectObjectForState = (l: LastProcessedNumber, _: Keys) => selectObjectForInstant(l)
    )
  )

  val program: ZIO[zio.s3.S3 with Blocking with Clock with KafkaConfig, TamerError, Unit] = for {
    _ <- new TamerS3Impl().fetch(setup)
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    mkS3KafkaLayer.flatMap(layer => program.provideCustomLayer(layer)).exitCode
}
