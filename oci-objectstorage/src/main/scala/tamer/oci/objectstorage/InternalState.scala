package tamer.oci.objectstorage

import tamer.AvroCodec

final case class InternalState(files: List[String], lastFileName: Option[String])

final case class LastProcessedFile(fileName: Option[String])
object LastProcessedFile {
  implicit val codec = AvroCodec.codec[LastProcessedFile]
}
