package tamer
package s3

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import eu.timepit.refined.types.numeric.PosInt
import zio.stream.Transducer

import java.time.Duration
import java.time.format.DateTimeFormatter
import scala.util.hashing.MurmurHash3.stringHash

final case class Setup[V <: Product: Encoder: Decoder: SchemaFor](
    bucketName: String,
    prefix: String,
    afterwards: LastProcessedInstant,
    transducer: Transducer[TamerError, Byte, V],
    parallelism: PosInt,
    dateTimeFormatter: DateTimeFormatter,
    minimumIntervalForBucketFetch: Duration
) extends _root_.tamer.Setup[S3Object, V, LastProcessedInstant](
      Serde[S3Object](isKey = true).serializer,
      Serde[V]().serializer,
      Serde[LastProcessedInstant]().serde,
      defaultState = afterwards,
      stringHash(bucketName) + stringHash(prefix) + afterwards.instant.getEpochSecond.intValue
    )
object Setup {
  final def fromDateTimeFormatter[V <: Product: Encoder: Decoder: SchemaFor](
      bucketName: String,
      filePathPrefix: String,
      afterwards: LastProcessedInstant,
      transducer: Transducer[TamerError, Byte, V],
      parallelism: PosInt,
      dateTimeFormatter: DateTimeFormatter,
      minimumIntervalForBucketFetch: Duration
  ): Setup[V] = Setup[V](bucketName, filePathPrefix, afterwards, transducer, parallelism, dateTimeFormatter, minimumIntervalForBucketFetch)
}
