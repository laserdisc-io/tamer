package tamer.oci.objectstorage

import com.sksamuel.avro4s.Codec
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.TamerError
import tamer.config.KafkaConfig
import tamer.job.{AbstractStatefulSourceTamerJob, SourceStateChanged}
import zio.{Chunk, Queue, Ref, Schedule, Task, ZIO}
import zio.ZIO.when
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.oci.objectstorage.{ListObjectsOptions, ObjectStorage}

import java.time.Duration

class TamerOciObjectStorageJob[
    R <: ObjectStorage with Blocking with Clock with KafkaConfig,
    K <: Product: Codec,
    V <: Product: Codec,
    S <: Product: Codec
](
    setup: OciObjectStorageConfiguration[R, K, V, S]
) extends AbstractStatefulSourceTamerJob[R, K, V, S, InternalState](setup.generic) {
  private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.oci.objectstorage")

  protected def createInitialSourceState: InternalState = InternalState(Nil, None)

  protected def createSchedule: Schedule[Any, Any, (Duration, Long)] = Schedule.exponential(5.minutes) || Schedule.spaced(5.minutes)

  protected def updatedSourceState(currentState: Ref[InternalState], token: Queue[Unit]): ZIO[R, Throwable, SourceStateChanged] =
    updateNamesRef(setup.namespace, setup.bucketName, setup.prefix, 5.minutes, currentState, token)

  private def updateNamesRef(
      namespace: String,
      bucketName: String,
      prefix: Option[String],
      timeout: Duration,
      state: Ref[InternalState],
      token: Queue[Unit]
  ): ZIO[ObjectStorage with Clock, Throwable, SourceStateChanged] =
    for {
      log                  <- logTask
      currentState         <- state.get
      _                    <- log.info(s"listing objects in bucket $bucketName of namespace $namespace")
      initialObjectListing <- zio.oci.objectstorage.listObjects(namespace, bucketName, ListObjectsOptions(prefix, currentState.lastFileName, 1))
      allObjectsListing    <- zio.oci.objectstorage.paginateObjects(initialObjectListing).take(3).timeout(timeout).runCollect.map(_.toList)
      fileNames         = allObjectsListing.flatMap(_.objectSummaries).map(_.getName())
      matchingFileNames = prefix.fold(fileNames)(p => fileNames.filter(_.startsWith(p)))
      lastFileName      = allObjectsListing.lastOption.flatMap(_.objectSummaries.lastOption.map(_.getName()))
      prevFileNames <- state.getAndSet(InternalState(matchingFileNames, lastFileName))
      filesChanged = prevFileNames.files != matchingFileNames
      _ <- when(filesChanged)(log.debug("File names changed") *> token.offer(()))
    } yield SourceStateChanged(filesChanged)

  protected def iteration(
      keysR: Ref[InternalState],
      keysChangedToken: Queue[Unit]
  )(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R with ObjectStorage, TamerError, S] =
    (for {
      log       <- logTask
      nextState <- setup.transitions.getNextState(keysR, currentState, keysChangedToken)
      keys      <- keysR.get
      optKey = keys.files.headOption
      _ <- log.debug(s"getting object $optKey from bucket ${setup.bucketName} in namespace ${setup.namespace}") *> optKey
        .map(key =>
          zio.oci.objectstorage
            .getObject(setup.namespace, setup.bucketName, key)
            .transduce(setup.transducer)
            .map(value => (setup.transitions.deriveKafkaRecordKey(currentState, value), value))
            .foreachChunk(q.offer)
        )
        .getOrElse(ZIO.fail(TamerError(s"error getting object $optKey from bucket ${setup.bucketName} in namespace ${setup.namespace}")))
      _ <- keysR.getAndSet(keys.copy(files = keys.files.tail))
    } yield nextState).mapError(e => TamerError(e.getLocalizedMessage, e))
}

object TamerOciObjectStorageJob {
  def apply[
      R <: ObjectStorage with Blocking with Clock with KafkaConfig,
      K <: Product: Codec,
      V <: Product: Codec,
      S <: Product: Codec
  ](
      setup: OciObjectStorageConfiguration[R, K, V, S]
  ) = new TamerOciObjectStorageJob[R, K, V, S](setup)
}
