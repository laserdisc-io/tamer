package tamer
package registry

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.avro.{Schema, SchemaValidatorBuilder}
import zio.{RIO, Task}

import scala.jdk.CollectionConverters._

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
      private[this] final val strategy                       = new SchemaValidatorBuilder().canReadStrategy().validateLatest()
      private[this] final def validate(toValidate: Schema, writerSchema: Schema): Task[Unit] =
        Task(strategy.validate(toValidate, List(writerSchema).asJava))

      override final def getOrRegisterId(subject: String, schema: Schema): Task[Int] =
        for {
          log <- logTask
          id <- Task(client.getId(subject, schema)).tap(id => log.debug(s"retrieved existing writer schema id: $id")) <>
                 Task(client.register(subject, schema)).tap(id => log.info(s"registered with id $id new subject $subject writer schema $schema"))
        } yield id
      override final def verifySchema(id: Int, schema: Schema): Task[Unit] =
        for {
          log          <- logTask
          writerSchema <- Task(client.getById(id)).tap(_ => log.debug(s"retrieved writer schema id: $id"))
          _            <- validate(schema, writerSchema).tapError(t => log.error(s"schema supplied cannot read payload: ${t.getLocalizedMessage}"))
        } yield ()
    }
  }
}
