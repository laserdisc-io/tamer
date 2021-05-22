package tamer
package s3

import eu.timepit.refined.api.RefType
import eu.timepit.refined.auto._
import software.amazon.awssdk.regions.Region
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.s3.S3
import zio.stream.Transducer

import java.net.URI
import scala.concurrent.duration.{FiniteDuration => ScalaFD}
import scala.util.hashing.MurmurHash3.stringHash

object S3Generalized extends App {
  object internals {
    val bucketName = "myBucket"
    val prefix     = "myFolder2/myPrefix"
    val transducer = Transducer.utf8Decode >>> Transducer.splitLines

    def getNextNumber(keysR: KeysR, afterwards: Long): UIO[Option[Long]] =
      keysR.get.map { keys =>
        val sortedFileNumbers = keys.map(_.stripPrefix(prefix).toLong).filter(afterwards < _)
        if (sortedFileNumbers.isEmpty) None else Some(sortedFileNumbers.min)
      }

    def getNextState(keysR: KeysR, afterwards: Long, keysChangedToken: Queue[Unit]): UIO[Long] = {
      val retryAfterWaitingForKeyListChange = keysChangedToken.take *> getNextState(keysR, afterwards, keysChangedToken)
      getNextNumber(keysR, afterwards).flatMap {
        case Some(number) if number > afterwards => UIO(number)
        case _                                   => retryAfterWaitingForKeyListChange
      }
    }

    def selectObjectForInstant(lastProcessedNumber: Long): Option[String] =
      Some(s"$prefix$lastProcessedNumber")
  }

  implicit final val longCodec   = AvroCodec.codec[Long]
  implicit final val stringCodec = AvroCodec.codec[String]

  private val myS3Setup: S3Setup[S3 with Blocking with Clock with Has[KafkaConfig], Long, String, Long] =
    S3Setup(
      bucketName = internals.bucketName,
      prefix = internals.prefix,
      tamerStateKafkaRecordKey = stringHash(internals.bucketName) + stringHash(internals.prefix) + 0,
      transducer = internals.transducer,
      parallelism = 1,
      S3Setup.S3PollingTimings(
        minimumIntervalForBucketFetch = 1.second,
        maximumIntervalForBucketFetch = 1.minute
      ),
      S3Setup.State(
        initialState = 0,
        getNextState = internals.getNextState,
        deriveKafkaRecordKey = (l: Long, _: String) => l,
        selectObjectForState = (l: Long, _: Keys) => internals.selectObjectForInstant(l)
      )
    )

  private val myProgram: ZIO[Blocking with Clock with S3 with Has[KafkaConfig], TamerError, Unit] =
    S3Tamer(myS3Setup).run.unit

  private lazy val myKafkaConfigLayer = ZIO
    .fromEither {
      val kafkaSink  = SinkConfig("sink-topic")
      val kafkaState = StateConfig("state-topic", "groupid", "clientid")
      RefType.applyRef[HostList](List("localhost:9092")).map { hostList =>
        KafkaConfig(hostList, "http://localhost:8081", ScalaFD(10, "seconds"), 50, kafkaSink, kafkaState)
      }
    }
    .mapError(TamerError(_))
    .toLayer
  private lazy val myS3Layer   = s3.liveM(Region.AF_SOUTH_1, s3.providers.default, Some(new URI("http://localhost:9000")))
  private lazy val myFullLayer = myS3Layer ++ myKafkaConfigLayer

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = myProgram.provideCustomLayer(myFullLayer).exitCode
}
