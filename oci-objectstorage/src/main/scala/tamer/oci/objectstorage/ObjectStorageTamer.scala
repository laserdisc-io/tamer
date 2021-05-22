package tamer
package oci.objectstorage

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio.{Chunk, Has, Queue, Task, ZIO}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.oci.objectstorage.{Limit, ListObjectsOptions, ObjectStorage, getObject, listObjects}

object ObjectStorageTamer {
  def apply[R <: Blocking with Clock with ObjectStorage with Has[KafkaConfig], K, V, S](setup: ObjectStorageSetup[R, K, V, S]) =
    new ObjectStorageTamer[R, K, V, S](setup)
}

class ObjectStorageTamer[R <: Blocking with Clock with ObjectStorage with Has[KafkaConfig], K, V, S](setup: ObjectStorageSetup[R, K, V, S])
    extends AbstractTamer[R, K, V, S](setup.generic) {

  private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.oci.objectstorage")

  override protected def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] = {
    val logic: ZIO[R with ObjectStorage, Throwable, S] = for {
      log <- logTask
      _   <- log.debug(s"current state: $currentState")
      startAfter = setup.objectNameBuilder.startAfter(currentState)
      nextObject <- listObjects(setup.namespace, setup.bucketName, ListObjectsOptions(setup.prefix, None, startAfter, Limit.Max))
      newState <- setup.transitions
        .getNextState(currentState, nextObject.objectSummaries.find(os => setup.objectNameFinder(os.getName)).map(_.getName))
      _ <- setup.objectNameBuilder.objectName(currentState) match {
        case Some(name) =>
          log.info(s"getting object $name") *> getObject(setup.namespace, setup.bucketName, name)
            .transduce(setup.transducer)
            .map(value => (setup.transitions.deriveKafkaRecordKey(currentState, value), value))
            .foreachChunk(q.offer)
        case None =>
          log.debug("no state change")
      }
    } yield newState

    logic.mapError(e => TamerError(e.getLocalizedMessage(), e))
  }
}
