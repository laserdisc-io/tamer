package tamer

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.avro.Schema
import zio._

trait Registry {
  def getOrRegisterId(subject: String, schema: Schema): Task[Int]
  def verifySchema(id: Int, schema: Schema): Task[Unit]
}

object Registry {
  val live: URLayer[Has[SchemaRegistryClient], Has[Registry]] = ZLayer.fromService { client =>
    new Registry {
      private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.registry")

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
