package tamer
package oci.objectstorage

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._
import zio.blocking.Blocking
import zio.oci.objectstorage.{Limit, ListObjectsOptions, ObjectStorage, getObject, listObjects}
import zio.stream.ZTransducer

trait ObjectNameBuilder[-S] {
  def startAfter(state: S): Option[String]
  def objectName(state: S): Option[String]
}

sealed abstract case class ObjectStorageSetup[-R, K, V, S](
    namespace: String,
    bucketName: String,
    prefix: Option[String],
    objectNameFinder: String => Boolean,
    objectNameBuilder: ObjectNameBuilder[S],
    serdes: Setup.Serdes[K, V, S],
    defaultState: S,
    recordKey: (S, V) => K,
    stateFold: (S, Option[String]) => URIO[R, S],
    transducer: ZTransducer[R, Throwable, Byte, V]
) extends Setup[R with Blocking with ObjectStorage, K, V, S] {
  override final val stateKey = namespace.hash + bucketName.hash + prefix.getOrElse("").hash
  override final val repr =
    s"""namespace: $namespace
       |bucket:    $bucketName
       |prefix:    $prefix
       |""".stripMargin

  private[this] final val logTask = log4sFromName.provide("tamer.oci.objectstorage")

  private[this] final def process(log: LogWriter[Task], currentState: S, queue: Queue[Chunk[(K, V)]]) =
    objectNameBuilder.objectName(currentState) match {
      case Some(name) =>
        log.info(s"getting object $name") *> getObject(namespace, bucketName, name)
          .transduce(transducer)
          .map(value => recordKey(currentState, value) -> value)
          .foreachChunk(queue.offer)
      case None =>
        log.debug("no state change")
    }

  override def iteration(currentState: S, queue: Queue[Chunk[(K, V)]]): RIO[R with Blocking with ObjectStorage, S] = for {
    log        <- logTask
    _          <- log.debug(s"current state: $currentState")
    options    <- UIO(ListObjectsOptions(prefix, None, objectNameBuilder.startAfter(currentState), Limit.Max))
    nextObject <- listObjects(namespace, bucketName, options)
    _          <- process(log, currentState, queue)
    newState   <- stateFold(currentState, nextObject.objectSummaries.find(os => objectNameFinder(os.getName)).map(_.getName))
  } yield newState
}

object ObjectStorageSetup {
  def apply[R, K: Codec, V: Codec, S: Codec](
      namespace: String,
      bucketName: String,
      defaultState: S,
      objectNameBuilder: ObjectNameBuilder[S]
  )(
      recordKey: (S, V) => K,
      stateFold: (S, Option[String]) => URIO[R, S],
      prefix: Option[String] = None,
      objectNameFinder: String => Boolean = _ => true,
      transducer: ZTransducer[R, Throwable, Byte, V] = ZTransducer.utf8Decode >>> ZTransducer.splitLines
  ): ObjectStorageSetup[R, K, V, S] = new ObjectStorageSetup[R, K, V, S](
    namespace,
    bucketName,
    prefix,
    objectNameFinder,
    objectNameBuilder,
    Setup.Serdes[K, V, S],
    defaultState,
    recordKey,
    stateFold,
    transducer
  ) {}
}
