package tamer

import org.apache.avro.Schema
import zio.{Has, RIO, URIO}

package object registry {
  final type TopicName    = String
  final type RegistryInfo = Has[Registry] with Has[TopicName]

  final val topic: URIO[Has[TopicName], String]                                       = URIO.service
  final def getOrRegisterId(subject: String, schema: Schema): RIO[Has[Registry], Int] = RIO.accessM(_.get.getOrRegisterId(subject, schema))
  final def verifySchema(id: Int, schema: Schema): RIO[Has[Registry], Unit]           = RIO.accessM(_.get.verifySchema(id, schema))
}
