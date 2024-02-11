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
    KafkaConfig(
      List("localhost:9092"),
      Some(RegistryConfig("http://localhost:8081")),
      10.seconds,
      50,
      TopicConfig("sink", Some(TopicOptions(1, 1, false))),
      TopicConfig("state", Some(TopicOptions(1, 1, true))),
      "groupid",
      "clientid",
      "s3-generalized-id"
    )
  }

  override final val run = S3Setup(
    bucket = internals.bucketName,
    prefix = internals.prefix,
    minimumIntervalForBucketFetch = 1.second,
    maximumIntervalForBucketFetch = 1.minute,
    initialState = 0L
  )(
    recordFrom = (l: Long, v: String) => Record(l, v),
    selectObjectForState = (l: Long, _: Keys) => internals.selectObjectForInstant(l),
    stateFold = internals.getNextState
  ).runWith(liveZIO(AF_SOUTH_1, ZIO.scoped(s3.providers.default), Some(new URI("http://localhost:9000"))) ++ myKafkaConfigLayer)
}
