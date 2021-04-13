package tamer.oci.objectstorage

import com.sksamuel.avro4s.Codec
import tamer.{SourceConfiguration, TamerError}
import zio.URIO
import zio.stream.ZTransducer

import scala.util.hashing.MurmurHash3.stringHash

trait ObjectNameBuilder[-S] {
  def startAfter(state: S): Option[String]
  def objectName(state: S): Option[String]
}

object OciObjectStorageConfiguration {
  case class State[R, K, V, S](
      initialState: S,
      getNextState: (S, Option[String]) => URIO[R, S],
      deriveKafkaRecordKey: (S, V) => K
  )
}

final case class OciObjectStorageConfiguration[
    R,
    K: Codec,
    V: Codec,
    S: Codec
](
    namespace: String,
    bucketName: String,
    prefix: Option[String],
    objectNameBuilder: ObjectNameBuilder[S],
    transitions: OciObjectStorageConfiguration.State[R, K, V, S],
    transducer: ZTransducer[R, TamerError, Byte, V]
) {
  private val repr: String =
    s"""
       |namespace: $namespace
       |bucket:    $bucketName
       |prefix:    $prefix
       |""".stripMargin.stripLeading()

  val generic: SourceConfiguration[K, V, S] = SourceConfiguration(
    SourceConfiguration.SourceSerde[K, V, S](),
    transitions.initialState,
    stringHash(namespace) + stringHash(bucketName) + stringHash(prefix.getOrElse("")),
    repr
  )
}
