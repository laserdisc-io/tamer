/*
 * Copyright (c) 2019-2025 LaserDisc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package tamer
package oci.objectstorage

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._

import zio.oci.objectstorage._
import zio.stream.ZPipeline

sealed abstract case class ObjectStorageSetup[-R, K: Tag, V: Tag, SV: Tag](
    initialState: SV,
    recordFrom: (SV, V) => Record[K, V],
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
      queue: Enqueue[NonEmptyChunk[Record[K, V]]]
  ): RIO[R with ObjectStorage, Unit] =
    objectName(currentState) match {
      case Some(name) =>
        log.info(s"getting object $name") *>
          getObject(namespace, bucket, name)
            .via(pipeline)
            .mapError(error => TamerError(s"Error while processing object $name: ${error.getMessage}", error))
            .map(recordFrom(currentState, _))
            .runForeachChunk(chunk => NonEmptyChunk.fromChunk(chunk).map(queue.offer).getOrElse(ZIO.unit))
      case None =>
        log.debug("no state change")
    }

  override def iteration(currentState: SV, queue: Enqueue[NonEmptyChunk[Record[K, V]]]): RIO[R with ObjectStorage, SV] = for {
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
      recordFrom: (SV, V) => Record[K, V],
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
    recordFrom,
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
