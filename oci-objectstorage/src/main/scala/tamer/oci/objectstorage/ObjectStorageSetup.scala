package tamer
package oci.objectstorage

import com.sksamuel.avro4s.Codec
import zio.URIO
import zio.stream.ZTransducer

import scala.util.hashing.MurmurHash3.stringHash

trait ObjectNameBuilder[-S] {
  def startAfter(state: S): Option[String]
  def objectName(state: S): Option[String]
}

object ObjectStorageSetup {
  case class State[R, K, V, S](
      initialState: S,
      getNextState: (S, Option[String]) => URIO[R, S],
      deriveKafkaRecordKey: (S, V) => K
  )
}

final case class ObjectStorageSetup[R, K: Codec, V: Codec, S: Codec](
    namespace: String,
    bucketName: String,
    prefix: Option[String],
    objectNameFinder: String => Boolean,
    objectNameBuilder: ObjectNameBuilder[S],
    transitions: ObjectStorageSetup.State[R, K, V, S],
    transducer: ZTransducer[R, TamerError, Byte, V]
) {
  private val repr: String =
    s"""
       |namespace: $namespace
       |bucket:    $bucketName
       |prefix:    $prefix
       |""".stripMargin.stripLeading()

  val generic: Setup[K, V, S] = Setup(
    Setup.Serdes[K, V, S],
    transitions.initialState,
    stringHash(namespace) + stringHash(bucketName) + stringHash(prefix.getOrElse("")),
    repr
  )
}
