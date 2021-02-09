package tamer
package s3

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import eu.timepit.refined.types.numeric.PosInt
import zio.stream.ZTransducer

import java.time.Duration
import scala.util.hashing.MurmurHash3.stringHash

final case class Setup[R, V <: Product: Encoder: Decoder: SchemaFor](
    bucketName: String,
    prefix: String,
    afterwards: LastProcessedInstant,
    transducer: ZTransducer[R, TamerError, Byte, V],
    parallelism: PosInt,
    zonedDateTimeFormatter: ZonedDateTimeFormatter,
    minimumIntervalForBucketFetch: Duration
) extends _root_.tamer.Setup[S3Object, V, LastProcessedInstant](
      Serde[S3Object](isKey = true).serializer,
      Serde[V]().serializer,
      Serde[LastProcessedInstant]().serde,
      defaultState = afterwards,
      stringHash(bucketName) + stringHash(prefix) + afterwards.instant.getEpochSecond.intValue
    )
object Setup {
  final def fromZonedDateTimeFormatter[R, V <: Product: Encoder: Decoder: SchemaFor](
      bucketName: String,
      filePathPrefix: String,
      afterwards: LastProcessedInstant,
      transducer: ZTransducer[R, TamerError, Byte, V],
      parallelism: PosInt,
      zonedDateTimeFormatter: ZonedDateTimeFormatter,
      minimumIntervalForBucketFetch: Duration
  ): Setup[R, V] = Setup[R, V](
    bucketName,
    filePathPrefix,
    afterwards,
    transducer,
    parallelism,
    zonedDateTimeFormatter,
    minimumIntervalForBucketFetch
  )
}
