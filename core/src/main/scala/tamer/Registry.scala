package tamer

import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

trait Registry {
  def getOrRegisterId(subject: String, schema: ParsedSchema): Task[Int]
  def verifySchema(id: Int, schema: ParsedSchema): Task[Unit]
}

object Registry {
  final object Fake extends Registry {
    override def getOrRegisterId(subject: String, schema: ParsedSchema): Task[Int] = UIO(-1)
    override def verifySchema(id: Int, schema: ParsedSchema): Task[Unit]           = UIO.unit
  }

  def live(url: String, size: Int = 1000, configuration: Map[String, Any] = Map.empty): Layer[TamerError, Has[Registry]] = ZLayer.fromEffect {
    Task(new CachedSchemaRegistryClient(url, size, configuration.asJava)).mapError(TamerError("Cannot construct registry client", _)).map { client =>
      new Registry {
        private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.registry")
        private[this] final def getId(subject: String, schema: ParsedSchema, log: LogWriter[Task]): Task[Int] =
          Task(client.getId(subject, schema)).tap(id => log.debug(s"retrieved existing writer schema id: $id"))
        private[this] final def register(subject: String, schema: ParsedSchema, log: LogWriter[Task]): Task[Int] =
          Task(client.register(subject, schema)).tap(id => log.info(s"registered with id $id new subject $subject writer schema $schema"))
        private[this] final def get(id: Int, log: LogWriter[Task]): Task[ParsedSchema] =
          Task(client.getSchemaById(id)).tap(_ => log.debug(s"retrieved writer schema id: $id"))
        @nowarn private[this] final def verify(schema: ParsedSchema, writerSchema: ParsedSchema): Task[Unit] =
          Task(schema.isBackwardCompatible(writerSchema).asScala)
            .filterOrElse(_.isEmpty) { errors =>
              ZIO.fail(TamerError(s"backwards incompatible schema: ${errors.mkString(", ")}"))
            }
            .unit

        override final def getOrRegisterId(subject: String, schema: ParsedSchema): Task[Int] = for {
          log <- logTask
          id  <- getId(subject, schema, log) <> register(subject, schema, log)
        } yield id
        override final def verifySchema(id: Int, schema: ParsedSchema): Task[Unit] = for {
          log          <- logTask
          writerSchema <- get(id, log)
          _            <- verify(schema, writerSchema)
        } yield ()
      }
    }
  }

  val fake: ULayer[Has[Registry]] = ZLayer.succeed(Fake)
}
