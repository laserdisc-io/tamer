package tamer
package example

import eu.timepit.refined.api.RefType
import eu.timepit.refined.auto._
import software.amazon.awssdk.regions.Region
import tamer.config._
import tamer.s3.{Keys, KeysR, S3Configuration, TamerS3Job}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.s3.S3
import zio.stream.Transducer

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

object S3Generalized extends App {
  object internals {
    val bucketName = "myBucket"
    val prefix     = "myFolder2/myPrefix"
    val transducer = Transducer.utf8Decode >>> Transducer.splitLines.map(Line.apply)

    def getNextNumber(keysR: KeysR, afterwards: LastProcessedNumber): UIO[Option[Long]] =
      keysR.get.map { keys =>
        val sortedFileNumbers = keys.map(_.stripPrefix(prefix).toLong).filter(afterwards.number < _)
        if (sortedFileNumbers.isEmpty) None else Some(sortedFileNumbers.min)
      }

    def getNextState(keysR: KeysR, afterwards: LastProcessedNumber, keysChangedToken: Queue[Unit] ): UIO[LastProcessedNumber] = {
      val retryAfterWaitingForKeyListChange = keysChangedToken.take *> getNextState(keysR, afterwards, keysChangedToken)
      getNextNumber(keysR, afterwards).flatMap {
        case Some(number) if number > afterwards.number => UIO(LastProcessedNumber(number))
        case _                                          => retryAfterWaitingForKeyListChange
      }
    }

    def selectObjectForInstant(lastProcessedNumber: LastProcessedNumber): Option[String] =
      Some(s"$prefix${lastProcessedNumber.number}")
  }

  private val myS3Configuration: S3Configuration[S3 with Blocking with Clock with KafkaConfig, LastProcessedNumber, Line, LastProcessedNumber] =
    S3Configuration(
      bucketName = internals.bucketName,
      prefix = internals.prefix,
      tamerStateKafkaRecordKey = stringHash(internals.bucketName) + stringHash(internals.prefix) + 0,
      transducer = internals.transducer,
      parallelism = 1,
      S3Configuration.S3PollingTimings(
        minimumIntervalForBucketFetch = 1.second,
        maximumIntervalForBucketFetch = 1.minute
      ),
      S3Configuration.State(
        initialState = LastProcessedNumber(0),
        getNextState = internals.getNextState,
        deriveKafkaRecordKey = (l: LastProcessedNumber, _: Line) => l,
        selectObjectForState = (l: LastProcessedNumber, _: Keys) => internals.selectObjectForInstant(l)
      )
    )

  private val myProgram: ZIO[Blocking with Clock with S3 with KafkaConfig, TamerError, Unit] =
    TamerS3Job(myS3Configuration).fetch().unit

  private lazy val myKafkaConfigLayer = ZIO.fromEither {
    val kafkaSink  = Config.KafkaSink("sink-topic")
    val kafkaState = Config.KafkaState("state-topic", "groupid", "clientid")
    RefType.applyRef[HostList](List("localhost:9092")).map { hostList =>
      Config.Kafka(hostList, "http://localhost:8081", ScalaFD(10, "seconds"), 50, kafkaSink, kafkaState)
    }
  }.mapError(TamerError(_)).toLayer
  private lazy val myS3Layer   = s3.liveM(Region.AF_SOUTH_1, s3.providers.default, Some(new URI("http://localhost:9000")))
  private lazy val myFullLayer = myS3Layer ++ myKafkaConfigLayer

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = myProgram.provideCustomLayer(myFullLayer).exitCode
}
