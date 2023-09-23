package tamer

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._

import scala.annotation.{implicitNotFound, nowarn}
import scala.jdk.CollectionConverters._

trait Registry[-S] {
  def getOrRegisterId(subject: String, schema: S): Task[Int]
  def verifySchema(id: Int, schema: S): Task[Unit]
}

object Registry {
  sealed abstract case class ConfluentRegistry(client: io.confluent.kafka.schemaregistry.client.SchemaRegistryClient, log: LogWriter[Task])
      extends Registry[io.confluent.kafka.schemaregistry.ParsedSchema] {
    override final def getOrRegisterId(subject: String, schema: io.confluent.kafka.schemaregistry.ParsedSchema): Task[Int] =
      getId(subject, schema) <> register(subject, schema)
    override final def verifySchema(id: Int, schema: io.confluent.kafka.schemaregistry.ParsedSchema): Task[Unit] =
      get(id).flatMap(verify(schema, _)).unit

    private[this] final def getId(subject: String, schema: io.confluent.kafka.schemaregistry.ParsedSchema): Task[Int] =
      ZIO.attemptBlocking(client.getId(subject, schema)).tap(id => log.debug(s"retrieved existing writer schema id: $id"))
    private[this] final def register(subject: String, schema: io.confluent.kafka.schemaregistry.ParsedSchema): Task[Int] =
      ZIO
        .attemptBlocking(client.register(subject, schema))
        .tap(id => log.info(s"registered with id $id new subject $subject writer schema $schema"))
    private[this] final def get(id: Int): Task[io.confluent.kafka.schemaregistry.ParsedSchema] =
      ZIO.attemptBlocking(client.getSchemaById(id)).tap(_ => log.debug(s"retrieved writer schema id: $id"))
    private[this] final def verify(
        schema: io.confluent.kafka.schemaregistry.ParsedSchema,
        writerSchema: io.confluent.kafka.schemaregistry.ParsedSchema
    ): Task[Unit] =
      ZIO
        .attemptBlocking(schema.isBackwardCompatible(writerSchema).asScala)
        .filterOrElseWith(_.isEmpty) { errors =>
          ZIO.fail(TamerError(s"Backwards incompatible schema: ${errors.mkString(", ")}"))
        }
        .unit
  }

  final object FakeRegistry extends Registry[Any] {
    override final def getOrRegisterId(subject: String, schema: Any): Task[Int] = ZIO.succeed(-1)
    override final def verifySchema(id: Int, schema: Any): Task[Unit]           = ZIO.unit
  }

  private[tamer] final def fakeRegistryZIO[RS]: ZIO[Scope, TamerError, Registry[RS]] = ZIO.succeed(Registry.FakeRegistry)
}

@implicitNotFound(
  "\n" +
    "Could not find or construct a \u001b[36mtamer.RegistryProvider\u001b[0m instance for type:\n" +
    "\n" +
    "  \u001b[32m${S}\u001b[0m\n" +
    "\n" +
    "There are a few provided OOTB, but consider creating one for the above types, if appropriate.\n"
)
sealed abstract case class RegistryProvider[S](from: RegistryConfig => ZIO[Scope, TamerError, Registry[S]])

object RegistryProvider extends LowPriorityRegistryProvider {
  final def apply[S](implicit S: RegistryProvider[S]): RegistryProvider[S] = S

  implicit final def confluentRegistryProvider[A: ConfluentSchemaRegistryClient]: RegistryProvider[io.confluent.kafka.schemaregistry.ParsedSchema] =
    new RegistryProvider(config =>
      ZIO
        .fromAutoCloseable(ZIO.attemptBlocking(new io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient(config.url, config.cacheSize)))
        .zip(log4sFromName.provideEnvironment(ZEnvironment("tamer.ConfluentRegistry")))
        .mapError(TamerError("Cannot construct registry client", _))
        .map { case (client, log) => new Registry.ConfluentRegistry(client, log) {} }
    ) {}
}

sealed trait LowPriorityRegistryProvider {
  implicit final def fakeRegistryProvider[A]: RegistryProvider[Any] = new RegistryProvider(_ => ZIO.succeed(Registry.FakeRegistry)) {}
}

private final abstract class ConfluentSchemaRegistryClient[SRC]
@nowarn private object ConfluentSchemaRegistryClient {
  @inline implicit final def get: ConfluentSchemaRegistryClient[io.confluent.kafka.schemaregistry.ParsedSchema] = null
}
