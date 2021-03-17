package tamer.oci.objectstorage

import com.sksamuel.avro4s.Codec
import tamer.{SourceConfiguration, TamerError}
import zio.{Queue, Ref, UIO}
import zio.stream.ZTransducer

final case class OciObjectStorageConfiguration[
    R,
    K <: Product: Codec,
    V <: Product: Codec,
    S <: Product: Codec
](
    namespace: String,
    bucketName: String,
    prefix: Option[String],
    transducer: ZTransducer[R, TamerError, Byte, V],
    transitions: OciObjectStorageConfiguration.State[K, V, S]
) {
  private val repr: String =
    s"""
       | namespace: $namespace
       | bucket: $bucketName
       |""".stripMargin.stripLeading()

  val generic: SourceConfiguration[K, V, S] = SourceConfiguration(
    SourceConfiguration.SourceSerde[K, V, S](),
    transitions.initialState,
    0,
    repr
  )
}

object OciObjectStorageConfiguration {
  case class State[K, V, S](initialState: S, getNextState: (Ref[InternalState], S, Queue[Unit]) => UIO[S], deriveKafkaRecordKey: (S, V) => K)
}
