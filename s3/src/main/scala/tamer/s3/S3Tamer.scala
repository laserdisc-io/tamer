package tamer
package s3

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.TamerError
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.s3.{ListObjectOptions, S3}
import zio.{Chunk, Has, Queue, Ref, Schedule, Task, ZIO}

import java.time.Duration
import scala.math.Ordering.Implicits.infixOrderingOps

class S3Tamer[R <: S3 with Blocking with Clock with Has[KafkaConfig], K, V, S](setup: S3Setup[R, K, V, S])
    extends AbstractStatefulTamer[R, K, V, S, Keys](setup.generic) {

  private final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.s3")

  override protected def createInitialSourceState: Keys = List.empty

  override protected def createSchedule: Schedule[Any, Any, (Duration, Long)] = Schedule.exponential(
    setup.pollingTimings.minimumIntervalForBucketFetch
  ) || Schedule.spaced(
    setup.pollingTimings.maximumIntervalForBucketFetch
  )

  override protected def updatedSourceState(currentState: Ref[Keys], token: Queue[Unit]): ZIO[R, Throwable, SourceStateChanged] =
    updateListOfKeys(
      currentState,
      setup.bucketName,
      setup.prefix,
      setup.pollingTimings.minimumIntervalForBucketFetch,
      token
    )

  private def updateListOfKeys(
      keysR: KeysR,
      bucketName: String,
      prefix: String,
      minimumIntervalForBucketFetch: Duration,
      keysChangedToken: Queue[Unit]
  ): ZIO[S3 with Clock, Throwable, SourceStateChanged] = {
    val paginationMaxKeys         = 1000L
    val paginationMaxPages        = 1000L
    val defaultTimeoutBucketFetch = 60.seconds
    val timeoutForFetchAllKeys: Duration =
      if (minimumIntervalForBucketFetch < defaultTimeoutBucketFetch) minimumIntervalForBucketFetch
      else defaultTimeoutBucketFetch

    def warnAboutSpuriousKeys(log: LogWriter[Task], keyList: List[String]) = {
      val witness = keyList.find(key => !key.startsWith(prefix)).getOrElse("")
      log.warn(s"""Server returned '$witness' (and maybe more files) which don't match prefix "$prefix"""")
    }

    for {
      log               <- logTask
      _                 <- log.info(s"getting list of keys in bucket $bucketName with prefix $prefix")
      initialObjListing <- zio.s3.listObjects(bucketName, ListObjectOptions.from(prefix, paginationMaxKeys))
      allObjListings <- zio.s3
        .paginate(initialObjListing)
        .take(paginationMaxPages)
        .timeout(timeoutForFetchAllKeys)
        .runCollect
        .map(_.toList)
      keyList = allObjListings
        .flatMap(objListing => objListing.objectSummaries)
        .map(_.key)
      cleanKeyList = keyList.filter(_.startsWith(prefix))
      _                  <- ZIO.when(keyList.size != cleanKeyList.size)(warnAboutSpuriousKeys(log, keyList))
      _                  <- log.debug(s"Current key list has ${cleanKeyList.length} elements")
      _                  <- log.debug(s"The first and last elements are ${cleanKeyList.sorted.headOption} and ${cleanKeyList.sorted.lastOption}")
      previousListOfKeys <- keysR.getAndSet(cleanKeyList)
      detectedKeyListChanged = cleanKeyList.sorted != previousListOfKeys.sorted
      _ <- ZIO.when(detectedKeyListChanged)(log.debug("Detected change in key list") *> keysChangedToken.offer(()))
    } yield if (detectedKeyListChanged) SourceStateChanged(true) else SourceStateChanged(false)
  }

  protected final def iteration(
      keysR: KeysR,
      keysChangedToken: Queue[Unit]
  )(
      currentState: S,
      q: Queue[Chunk[(K, V)]]
  ): ZIO[R with zio.s3.S3, TamerError, S] =
    (for {
      log       <- logTask
      nextState <- setup.transitions.getNextState(keysR, currentState, keysChangedToken)
      _         <- log.debug(s"Next state computed to be $nextState")
      keys      <- keysR.get
      optKey = setup.transitions.selectObjectForState(nextState, keys)
      _ <- log.debug(s"Will ask for key $optKey") *> optKey
        .map(key =>
          zio.s3
            .getObject(setup.bucketName, key)
            .transduce(setup.transducer)
            .map(value => (setup.transitions.deriveKafkaRecordKey(nextState, value), value))
            .foreachChunk(q.offer)
        )
        .getOrElse(ZIO.fail(TamerError(s"File not found with key $optKey for state $nextState"))) // FIXME: relies on nextState.toString
    } yield nextState).mapError(e => TamerError("Error while doing iterationTimeBased", e))
}

object S3Tamer {
  def apply[R <: S3 with Blocking with Clock with Has[KafkaConfig], K, V, S](setup: S3Setup[R, K, V, S]) = new S3Tamer(setup)
}
