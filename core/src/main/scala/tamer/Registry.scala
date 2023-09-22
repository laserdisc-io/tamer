package tamer

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

trait Registry {
  type S

  def getOrRegisterId(subject: String, schema: S): Task[Int]
  def verifySchema(id: Int, schema: S): Task[Unit]
}

object Registry {
  final type Aux[-S0] = Registry { type S >: S0 }

  final case class ConfluentRegistry(client: io.confluent.kafka.schemaregistry.client.SchemaRegistryClient, log: LogWriter[Task]) extends Registry {
    override type S = io.confluent.kafka.schemaregistry.ParsedSchema

    override final def getOrRegisterId(subject: String, schema: S): Task[Int] =
      getId(subject, schema) <> register(subject, schema)
    override final def verifySchema(id: Int, schema: S): Task[Unit] =
      get(id).flatMap(verify(schema, _)).unit

    private[this] final def getId(subject: String, schema: S): Task[Int] =
      ZIO.attemptBlocking(client.getId(subject, schema)).tap(id => log.debug(s"retrieved existing writer schema id: $id"))
    private[this] final def register(subject: String, schema: S): Task[Int] =
      ZIO
        .attemptBlocking(client.register(subject, schema))
        .tap(id => log.info(s"registered with id $id new subject $subject writer schema $schema"))
    private[this] final def get(id: Int): Task[S] =
      ZIO.attemptBlocking(client.getSchemaById(id)).tap(_ => log.debug(s"retrieved writer schema id: $id"))
    private[this] final def verify(schema: S, writerSchema: S): Task[Unit] =
      ZIO
        .attemptBlocking(schema.isBackwardCompatible(writerSchema).asScala)
        .filterOrElseWith(_.isEmpty) { errors =>
          ZIO.fail(TamerError(s"Backwards incompatible schema: ${errors.mkString(", ")}"))
        }
        .unit
  }

  final object FakeRegistry extends Registry {
    override type S = Any

    override final def getOrRegisterId(subject: String, schema: S): Task[Int] = ZIO.succeed(-1)
    override final def verifySchema(id: Int, schema: S): Task[Unit]           = ZIO.unit
  }

  final def fakeRegistryZIO[RS]: ZIO[Scope, TamerError, Registry.Aux[RS]] = ZIO.succeed(Registry.FakeRegistry)
}

sealed abstract case class RegistryProvider[S](from: RegistryConfig => ZIO[Scope, TamerError, Registry.Aux[S]])

object RegistryProvider extends LowPriorityRegistryProvider {

  implicit final def confluentRegistryProvider[A: ConfluentSchemaRegistryClient]: RegistryProvider[io.confluent.kafka.schemaregistry.ParsedSchema] =
    new RegistryProvider(config =>
      ZIO
        .fromAutoCloseable(ZIO.attemptBlocking(new io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient(config.url, config.cacheSize)))
        .zip(log4sFromName.provideEnvironment(ZEnvironment("tamer.LiveConfluentRegistry")))
        .mapError(TamerError("Cannot construct registry client", _))
        .map { case (client, log) => new Registry.ConfluentRegistry(client, log) }
    ) {}
}

sealed trait LowPriorityRegistryProvider {
  implicit final def fakeRegistryProvider[A]: RegistryProvider[Any] = new RegistryProvider(_ => Registry.fakeRegistryZIO) {}
}

private final abstract class ConfluentSchemaRegistryClient[SRC]
@nowarn private object ConfluentSchemaRegistryClient {
  @inline implicit final def get: ConfluentSchemaRegistryClient[io.confluent.kafka.schemaregistry.ParsedSchema] = null
}
