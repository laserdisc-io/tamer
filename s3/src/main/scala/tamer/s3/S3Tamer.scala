package tamer
package s3

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.s3.{ListObjectOptions, S3}
import zio.{Chunk, Has, Queue, Ref, Schedule, Task, UIO, ZIO}

import java.time.Duration
import scala.math.Ordering.Implicits.infixOrderingOps

class S3Tamer[R <: S3 with Blocking with Clock with Has[KafkaConfig], K, V, S](setup: S3Setup[R, K, V, S])
    extends AbstractTamer[R, K, V, S](setup.generic) {

  private final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.s3")

  private final val initialSourceState: UIO[KeysR] = Ref.make(List.empty)

  private final val createSchedule: Schedule[Any, Any, (Duration, Long)] = Schedule.exponential(
    setup.pollingTimings.minimumIntervalForBucketFetch
  ) || Schedule.spaced(
    setup.pollingTimings.maximumIntervalForBucketFetch
  )

  private final def updatedSourceState(currentState: Ref[Keys], token: Queue[Unit]): ZIO[R, Throwable, SourceStateChanged] =
    updateListOfKeys(
      currentState,
      setup.bucketName,
      setup.prefix,
      setup.pollingTimings.minimumIntervalForBucketFetch,
      token
    )

  private final def updateListOfKeys(
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

  override protected final def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] =
    for {
      sourceState <- initialSourceState
      cappedExponentialBackoff = createSchedule
      sourceStateChangedToken <- Queue.dropping[Unit](requestedCapacity = 1)
      newSourceState = updatedSourceState(sourceState, sourceStateChangedToken)
      _ <- newSourceState
        .scheduleFrom(SourceStateChanged(true))(
          Schedule.once andThen cappedExponentialBackoff.untilInput(_ == SourceStateChanged(true))
        )
        .forever
        .fork
      newState <- iteration(sourceState, sourceStateChangedToken)(currentState, q)
    } yield newState
}

object S3Tamer {
  def apply[R <: S3 with Blocking with Clock with Has[KafkaConfig], K, V, S](setup: S3Setup[R, K, V, S]) = new S3Tamer(setup)
}
