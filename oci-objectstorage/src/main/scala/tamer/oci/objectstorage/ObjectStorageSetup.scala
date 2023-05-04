package tamer
package oci.objectstorage

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._
import zio.blocking.Blocking
import zio.oci.objectstorage._
import zio.stream.{ZStream, ZTransducer}

sealed abstract case class ObjectStorageSetup[-R, K, V, S](
    serdes: Setup.Serdes[K, V, S],
    initialState: S,
    recordKey: (S, V) => K,
    namespace: String,
    bucket: String,
    prefix: Option[String],
    objectName: S => Option[String],
    startAfter: S => Option[String],
    objectNameFinder: String => Boolean,
    stateFold: (S, Option[String]) => URIO[R, S],
    transducer: ZTransducer[R, Throwable, Byte, V]
) extends Setup[R with Blocking with ObjectStorage, K, V, S] {

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

  private[this] final val logTask = log4sFromName.provide("tamer.oci.objectstorage")

  private[this] final def process(
                                   log: LogWriter[Task],
                                   currentState: S,
                                 ): ZStream[ObjectStorage with Blocking with R, Throwable, Option[NonEmptyChunk[(K, V)]]] =
    objectName(currentState) match {
      case Some(name) =>
          ZStream.fromEffect(log.info(s"getting object $name")) *>
          getObject(namespace, bucket, name)
            .transduce(transducer)
            .mapBoth(
              error => TamerError(s"Error while processing object $name: ${error.getMessage}", error),
              value => Chunk(recordKey(currentState, value) -> value)
            ).map(ch => NonEmptyChunk.fromChunk(ch))
      case None =>
        ZStream.empty
    }

  override def iteration(currentState: S): ZStream[R with Blocking with ObjectStorage, Throwable, (Option[NonEmptyChunk[(K, V)]], S)] = {
    ZStream.fromEffect(logTask).flatMap { logger =>
      ZStream.fromEffect {
        val options = ListObjectsOptions(prefix, None, startAfter(currentState), Limit.Max, Set(ListObjectsOptions.Field.Name, ListObjectsOptions.Field.Size))
        logger.debug(s"current state: $currentState") *> listObjects(namespace, bucket, options)
      }.flatMap { nextObject =>
        val newState = stateFold(currentState, nextObject.objectSummaries.find(os => objectNameFinder(os.getName)).map(_.getName))
        ZStream.fromEffect(newState).flatMap { nState =>
          process(logger, currentState).map(c => (c, nState))
        }
      }
    }
  }
}

object ObjectStorageSetup {
  def apply[R, K: Codec, V: Codec, S: Codec](
      namespace: String,
      bucket: String,
      initialState: S
  )(
      recordKey: (S, V) => K,
      stateFold: (S, Option[String]) => URIO[R, S],
      objectName: S => Option[String],
      startAfter: S => Option[String],
      prefix: Option[String] = None,
      objectNameFinder: String => Boolean = _ => true,
      transducer: ZTransducer[R, Throwable, Byte, V] = ZTransducer.utf8Decode >>> ZTransducer.splitLines
  )(
      implicit ev: Codec[Tamer.StateKey]
  ): ObjectStorageSetup[R, K, V, S] = new ObjectStorageSetup(
    Setup.mkSerdes[K, V, S],
    initialState,
    recordKey,
    namespace,
    bucket,
    prefix,
    objectName,
    startAfter,
    objectNameFinder,
    stateFold,
    transducer
  ) {}
}
