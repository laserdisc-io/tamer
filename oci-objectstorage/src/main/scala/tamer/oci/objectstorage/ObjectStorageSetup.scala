package tamer
package oci.objectstorage

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._

import zio.oci.objectstorage._
import zio.stream.ZPipeline

sealed abstract case class ObjectStorageSetup[-R, K: Tag, V: Tag, SV: Tag](
    initialState: SV,
    recordKey: (SV, V) => K,
    namespace: String,
    bucket: String,
    prefix: Option[String],
    objectName: SV => Option[String],
    startAfter: SV => Option[String],
    objectNameFinder: String => Boolean,
    stateFold: (SV, Option[String]) => URIO[R, SV],
    pipeline: ZPipeline[R, Throwable, Byte, V]
)(
    implicit ev: SerdesProvider[K, V, SV]
) extends Setup[R with ObjectStorage, K, V, SV] {

  private[this] final val namespaceHash = namespace.hash
  private[this] final val bucketHash    = bucket.hash
  private[this] final val prefixHash    = prefix.getOrElse("").hash

  override final val stateKey = namespaceHash + bucketHash + prefixHash
  override final val repr =
    s"""namespace:      $namespace
       |namespace hash: $namespaceHash
       |bucket:         $bucket
       |bucket hash:    $bucketHash
       |prefix:         $prefix
       |prefix hash:    $prefixHash
       |state key:      $stateKey
       |""".stripMargin

  private[this] final val logTask = log4sFromName.provideEnvironment(ZEnvironment("tamer.oci.objectstorage"))

  private[this] final def process(
      log: LogWriter[Task],
      currentState: SV,
      queue: Enqueue[NonEmptyChunk[(K, V)]]
  ): RIO[R with ObjectStorage, Unit] =
    objectName(currentState) match {
      case Some(name) =>
        log.info(s"getting object $name") *>
          getObject(namespace, bucket, name)
            .via(pipeline)
            .mapError(error => TamerError(s"Error while processing object $name: ${error.getMessage}", error))
            .map(value => recordKey(currentState, value) -> value)
            .runForeachChunk(chunk => NonEmptyChunk.fromChunk(chunk).map(queue.offer).getOrElse(ZIO.unit))
      case None =>
        log.debug("no state change")
    }

  override def iteration(currentState: SV, queue: Enqueue[NonEmptyChunk[(K, V)]]): RIO[R with ObjectStorage, SV] = for {
    log <- logTask
    _   <- log.debug(s"current state: $currentState")
    options <- ZIO.succeed(
      ListObjectsOptions(prefix, None, startAfter(currentState), Limit.Max, Set(ListObjectsOptions.Field.Name, ListObjectsOptions.Field.Size))
    )
    nextObject <- listObjects(namespace, bucket, options)
    _          <- process(log, currentState, queue)
    newState   <- stateFold(currentState, nextObject.objectSummaries.find(os => objectNameFinder(os.getName)).map(_.getName))
  } yield newState
}

object ObjectStorageSetup {
  def apply[R, K: Tag, V: Tag, SV: Tag](
      namespace: String,
      bucket: String,
      initialState: SV
  )(
      recordKey: (SV, V) => K,
      stateFold: (SV, Option[String]) => URIO[R, SV],
      objectName: SV => Option[String],
      startAfter: SV => Option[String],
      prefix: Option[String] = None,
      objectNameFinder: String => Boolean = _ => true,
      pipeline: ZPipeline[R, Throwable, Byte, V] = ZPipeline.utf8Decode >>> ZPipeline.splitLines
  )(
      implicit ev: SerdesProvider[K, V, SV]
  ): ObjectStorageSetup[R, K, V, SV] = new ObjectStorageSetup(
    initialState,
    recordKey,
    namespace,
    bucket,
    prefix,
    objectName,
    startAfter,
    objectNameFinder,
    stateFold,
    pipeline
  ) {}
}
