package tamer.oci.objectstorage

import com.sksamuel.avro4s.Codec
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import tamer.TamerError
import tamer.config.KafkaConfig
import tamer.job.AbstractTamerJob
import zio.{Chunk, Queue, Task, ZEnv, ZIO}
import zio.oci.objectstorage.{ListObjectsOptions, ObjectStorage, getObject, listObjects}

object TamerOciObjectStorageJob {
  def apply[
      R <: ZEnv with ObjectStorage with KafkaConfig,
      K <: Product: Codec,
      V <: Product: Codec,
      S <: Product: Codec
  ](
      setup: OciObjectStorageConfiguration[R, K, V, S]
  ) = new TamerOciObjectStorageJob[R, K, V, S](setup)
}

class TamerOciObjectStorageJob[
    R <: ZEnv with ObjectStorage with KafkaConfig,
    K <: Product: Codec,
    V <: Product: Codec,
    S <: Product: Codec
](
    setup: OciObjectStorageConfiguration[R, K, V, S]
) extends AbstractTamerJob[R, K, V, S](setup.generic) {
  private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.oci.objectstorage")

  override protected def next(currentState: S, q: Queue[Chunk[(K, V)]]): ZIO[R, TamerError, S] = {
    val logic: ZIO[R with ObjectStorage, Throwable, S] = for {
      log <- logTask
      _   <- log.debug(s"current state: $currentState")
      startAfter = setup.objectNameBuilder.startAfter(currentState)
      nextObject <- listObjects(setup.namespace, setup.bucketName, ListObjectsOptions(setup.prefix, None, startAfter, 1))
      newState   <- setup.transitions.getNextState(currentState, nextObject.objectSummaries.headOption.map(_.getName))
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
