package tamer
package registry

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.avro.Schema
import zio.{RIO, Task, UIO}

trait Registry extends Serializable {
  val registry: Registry.Service[Any]
}

object Registry {
  trait Service[R] {
    def getOrRegisterId(subject: String, schema: Schema): RIO[R, Int]
    def verifySchema(id: Int, schema: Schema): RIO[R, Unit]
  }

  object > extends Service[Registry] {
    override final def getOrRegisterId(subject: String, schema: Schema): RIO[Registry, Int] = RIO.accessM(_.registry.getOrRegisterId(subject, schema))
    override final def verifySchema(id: Int, schema: Schema): RIO[Registry, Unit]           = RIO.accessM(_.registry.verifySchema(id, schema))
  }

  trait Live extends Registry {
    val client: SchemaRegistryClient
    override final val registry: Service[Any] = new Service[Any] {
      private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.Registry.Live")

      override final def getOrRegisterId(subject: String, schema: Schema): Task[Int] =
        for {
          s   <- UIO.succeed(new AvroSchema(schema))
          log <- logTask
          id <-
            Task(client.getId(subject, s)).tap(id => log.debug(s"retrieved existing writer schema id: $id")) <>
              Task(client.register(subject, s)).tap(id => log.info(s"registered with id $id new subject $subject writer schema $schema"))
        } yield id
      override final def verifySchema(id: Int, schema: Schema): Task[Unit] =
        for {
          s            <- UIO.succeed(new AvroSchema(schema))
          log          <- logTask
          writerSchema <- Task(client.getSchemaById(id)).tap(_ => log.debug(s"retrieved writer schema id: $id"))
          _            <- Task(s.isBackwardCompatible(writerSchema)).tapError(t => log.error(s"schema supplied cannot read payload: ${t.getLocalizedMessage}"))
        } yield ()
    }
  }
}
