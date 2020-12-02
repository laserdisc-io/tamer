package tamer

import org.apache.avro.Schema
import zio.{Has, RIO, URIO}

package object registry {
  final type Registry = Has[Registry.Service]
  final type Topic    = Has[Topic.Service]

  final def topic: URIO[Topic, String]                                           = URIO.access(_.get.topic)
  final def getOrRegisterId(subject: String, schema: Schema): RIO[Registry, Int] = RIO.accessM(_.get.getOrRegisterId(subject, schema))
  final def verifySchema(id: Int, schema: Schema): RIO[Registry, Unit]           = RIO.accessM(_.get.verifySchema(id, schema))
}
