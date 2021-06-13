package tamer
package s3

import eu.timepit.refined.api.RefType
import eu.timepit.refined.auto._
import software.amazon.awssdk.regions.Region.AF_SOUTH_1
import zio._
import zio.duration._
import zio.s3._

import java.net.URI
import scala.concurrent.duration.FiniteDuration

object S3Generalized extends App {
  object internals {
    val bucketName = "myBucket"
    val prefix     = "myFolder2/myPrefix"

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

  val myKafkaConfigLayer = ZIO
    .fromEither {
      val kafkaSink  = SinkConfig("sink-topic")
      val kafkaState = StateConfig("state-topic", "groupid", "clientid")
      RefType.applyRef[HostList](List("localhost:9092")).map { hostList =>
        KafkaConfig(hostList, "http://localhost:8081", 10.seconds.asScala.asInstanceOf[FiniteDuration], 50, kafkaSink, kafkaState)
      }
    }
    .mapError(TamerError(_))
    .toLayer

  val program = S3Setup(
    bucketName = internals.bucketName,
    prefix = internals.prefix,
    defaultState = 0L,
    stateFold = internals.getNextState,
    recordKey = (l: Long, _: String) => l,
    selectObjectForState = (l: Long, _: Keys) => internals.selectObjectForInstant(l),
    minimumIntervalForBucketFetch = 1.second,
    maximumIntervalForBucketFetch = 1.minute
  ).runWith(liveM(AF_SOUTH_1, s3.providers.default, Some(new URI("http://localhost:9000"))) ++ myKafkaConfigLayer)

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}
