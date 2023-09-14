package tamer

import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._

import scala.jdk.CollectionConverters._

trait Registry[-A] {
  def getOrRegisterId(subject: String, schema: A): Task[Int]
  def verifySchema(id: Int, schema: A): Task[Unit]
}

object Registry {
  final object FakeRegistry extends Registry[Nothing] {
    override def getOrRegisterId(subject: String, schema: Nothing): Task[Int] = ZIO.succeed(-1)
    override def verifySchema(id: Int, schema: Nothing): Task[Unit]           = ZIO.unit
  }

  final case class LiveConfluentRegistry(private val client: SchemaRegistryClient) extends Registry[ParsedSchema] {
    private[this] final val logTask: Task[LogWriter[Task]] = log4sFromName.provideEnvironment(ZEnvironment("tamer.LiveConfluentRegistry"))
    private[this] final def getId(subject: String, schema: ParsedSchema, log: LogWriter[Task]): Task[Int] =
      ZIO.attemptBlocking(client.getId(subject, schema)).tap(id => log.debug(s"retrieved existing writer schema id: $id"))
    private[this] final def register(subject: String, schema: ParsedSchema, log: LogWriter[Task]): Task[Int] =
      ZIO.attemptBlocking(client.register(subject, schema)).tap(id => log.info(s"registered with id $id new subject $subject writer schema $schema"))
    private[this] final def get(id: Int, log: LogWriter[Task]): Task[ParsedSchema] =
      ZIO.attemptBlocking(client.getSchemaById(id)).tap(_ => log.debug(s"retrieved writer schema id: $id"))
    private[this] final def verify(schema: ParsedSchema, writerSchema: ParsedSchema): Task[Unit] =
      ZIO
        .attemptBlocking(schema.isBackwardCompatible(writerSchema).asScala)
        .filterOrElseWith(_.isEmpty) { errors =>
          ZIO.fail(TamerError(s"Backwards incompatible schema: ${errors.mkString(", ")}"))
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

  def liveConfluent(url: String, size: Int = 1000, configuration: Map[String, Any] = Map.empty): Layer[TamerError, Registry[ParsedSchema]] = ZLayer {
    ZIO
      .attemptBlocking(new CachedSchemaRegistryClient(url, size, configuration.asJava))
      .mapError(TamerError("Cannot construct registry client", _))
      .map(LiveConfluentRegistry)
  }

  val fake: ULayer[Registry[Nothing]] = ZLayer.succeed(FakeRegistry)
}
