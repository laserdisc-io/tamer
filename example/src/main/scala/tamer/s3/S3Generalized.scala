package tamer
package s3

import java.net.URI

import software.amazon.awssdk.regions.Region.AF_SOUTH_1
import zio._
import zio.s3._

object S3Generalized extends ZIOAppDefault {
  import implicits._

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
        case Some(number) if number > afterwards => ZIO.succeed(number)
        case _                                   => retryAfterWaitingForKeyListChange
      }
    }

    def selectObjectForInstant(lastProcessedNumber: Long): Option[String] =
      Some(s"$prefix$lastProcessedNumber")
  }

  val myKafkaConfigLayer = ZLayer.succeed {
    val kafkaSink  = SinkConfig("sink-topic")
    val kafkaState = StateConfig("state-topic", "groupid", "clientid")
    KafkaConfig(List("localhost:9092"), Some(RegistryConfig("http://localhost:8081")), 10.seconds, 50, kafkaSink, kafkaState, "s3-generalized-id")
  }

  override final val run = S3Setup(
    bucket = internals.bucketName,
    prefix = internals.prefix,
    minimumIntervalForBucketFetch = 1.second,
    maximumIntervalForBucketFetch = 1.minute,
    initialState = 0L
  )(
    recordKey = (l: Long, _: String) => l,
    selectObjectForState = (l: Long, _: Keys) => internals.selectObjectForInstant(l),
    stateFold = internals.getNextState
  ).runWith(Scope.default >>> (liveZIO(AF_SOUTH_1, s3.providers.default, Some(new URI("http://localhost:9000"))) ++ myKafkaConfigLayer)).exitCode
}
