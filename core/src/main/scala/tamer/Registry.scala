package tamer

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import zio._

import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters._

trait Registry[-S] {
  def getOrRegisterId(subject: String, schema: S): Task[Int]
  def verifySchema(id: Int, schema: S): Task[Unit]
}

object Registry {
  object SttpRegistry {
    import org.apache.avro._
    import org.apache.avro.SchemaCompatibility.Incompatibility
    import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType._
    import sttp.client3._
    import sttp.client3.json4s._
    import sttp.model._

    private[this] final case class SchemaString(schema: String)
    private[this] final object SchemaString {
      def apply(schema: Schema): SchemaString = SchemaString(schema.toString())
    }
    private[this] final case class SubjectIdVersionSchemaString(subject: String, id: Int, version: Int, schema: String)
    private[this] final case class Id(id: Int)
    private[this] final case class Difference(i: Incompatibility) {
      override final def toString(): String = {
        val errorDescription = i.getType() match {
          case FIXED_SIZE_MISMATCH =>
            s"The size of FIXED type field at path '${i.getLocation()}' in the reader schema (${i
                .getReaderFragment()}) does not match with the writer schema (${i.getWriterFragment()})"
          case TYPE_MISMATCH =>
            s"The type (path '${i.getLocation()}') of a field in the reader schema (${i
                .getReaderFragment()}) does not match with the writer schema (${i.getWriterFragment()})"
          case NAME_MISMATCH =>
            s"The name of the schema has changed (path '${i.getLocation()}')"
          case MISSING_ENUM_SYMBOLS =>
            s"The reader schema (${i.getReaderFragment()}) is missing enum symbols '${i.getMessage()}' at path '${i
                .getLocation()}' in the writer schema (${i.getWriterFragment()})"
          case MISSING_UNION_BRANCH =>
            s"The reader schema (${i.getReaderFragment()}) is missing a type inside a union field at path '${i
                .getLocation()}' in the writer schema (${i.getWriterFragment()})"
          case READER_FIELD_MISSING_DEFAULT_VALUE =>
            s"The field '${i.getMessage()}' at path '${i.getLocation()}' in the reader schema (${i
                .getReaderFragment()}) has no default value and is missing in the writer schema (${i.getWriterFragment()})"
        }
        s"{errorType:'${i.getType()}', description:'$errorDescription', additionalInfo:'${i.getMessage()}'}"
      }
    }

    private[this] implicit final val formats: org.json4s.DefaultFormats      = org.json4s.DefaultFormats
    private[this] implicit final val serialization: org.json4s.Serialization = org.json4s.jackson.Serialization

    private[this] final val schemaRegistryV1MediaType = MediaType.unsafeParse("application/vnd.schemaregistry.v1+json")
    private[this] final val schemaRegistryMediaType   = MediaType.unsafeParse("application/vnd.schemaregistry+json")
    private[this] final val request = basicRequest.headers(
      Header.accept(schemaRegistryV1MediaType, schemaRegistryMediaType, MediaType.ApplicationJson),
      Header.contentType(schemaRegistryV1MediaType)
    )
    private[this] final val schemaParser = new Schema.Parser()

    private[this] final def getId(backend: SttpBackend[Task, Any], url: String, log: LogWriter[Task], subject: String, schema: Schema): Task[Int] =
      request
        .post(uri"$url/subjects/$subject?normalize=false&deleted=false")
        .body(SchemaString(schema))
        .response(asJson[SubjectIdVersionSchemaString])
        .send(backend)
        .flatMap(response => ZIO.fromEither(response.body.map(_.id)))
        .tap(id => log.debug(s"retrieved existing writer schema id: $id"))
    private[this] final def register(backend: SttpBackend[Task, Any], url: String, log: LogWriter[Task], subject: String, schema: Schema): Task[Int] =
      request
        .post(uri"$url/subjects/$subject/versions?normalize=false")
        .body(SchemaString(schema))
        .response(asJson[Id])
        .send(backend)
        .flatMap(response => ZIO.fromEither(response.body.map(_.id)))
        .tap(id => log.info(s"registered with id $id new subject $subject writer schema $schema"))
    private[this] final def get(backend: SttpBackend[Task, Any], url: String, log: LogWriter[Task], id: Int): Task[Schema] =
      request
        .get(uri"$url/schemas/ids/$id?subject=")
        .response(asJson[SchemaString])
        .send(backend)
        .flatMap(response => ZIO.fromEither(response.body.map(_.schema)))
        .flatMap(schema => ZIO.attempt(schemaParser.parse(schema)))
        .tap(_ => log.debug(s"retrieved writer schema id: $id"))
    private[this] final def verify(schema: Schema, writerSchema: Schema): Task[Unit] =
      ZIO
        .attemptBlocking {
          SchemaCompatibility.checkReaderWriterCompatibility(schema, writerSchema).getResult().getIncompatibilities()
        }
        .map(_.asScala.map(Difference(_).toString).toList)
        .catchNonFatalOrDie(e => ZIO.succeed(s"Unexpected exception during compatibility check: ${e.getMessage()}" :: Nil))
        .filterOrElseWith(_.isEmpty) { errors =>
          ZIO.fail(TamerError(s"Backwards incompatible schema: ${errors.mkString(", ")}"))
        }
        .unit

    final def getOrRegisterId(backend: SttpBackend[Task, Any], url: String, log: LogWriter[Task], subject: String, schema: Schema): Task[Int] =
      getId(backend, url, log, subject, schema) <> register(backend, url, log, subject, schema)
    final def verifySchema(backend: SttpBackend[Task, Any], url: String, log: LogWriter[Task], id: Int, schema: Schema): Task[Unit] =
      get(backend, url, log, id).flatMap(verify(schema, _)).unit
  }
  sealed abstract class SttpRegistry(
      backend: sttp.client3.SttpBackend[Task, Any],
      url: String,
      schemaToIdCache: zio.cache.Cache[
        (sttp.client3.SttpBackend[Task, Any], String, LogWriter[Task], String, org.apache.avro.Schema),
        Throwable,
        Int
      ],
      schemaIdToValidationCache: zio.cache.Cache[
        (sttp.client3.SttpBackend[Task, Any], String, LogWriter[Task], Int, org.apache.avro.Schema),
        Throwable,
        Unit
      ],
      log: LogWriter[Task]
  ) extends Registry[org.apache.avro.Schema] {
    override final def getOrRegisterId(subject: String, schema: org.apache.avro.Schema): Task[Int] =
      schemaToIdCache.get((backend, url, log, subject, schema))
    override final def verifySchema(id: Int, schema: org.apache.avro.Schema): Task[Unit] =
      schemaIdToValidationCache.get((backend, url, log, id, schema))
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

  implicit final def sttpAvroRegistryProvider: RegistryProvider[org.apache.avro.Schema] = new RegistryProvider(config => {
    val sttpBackend = sttp.client3.httpclient.zio.HttpClientZioBackend.scoped()
    val schemaToIdCache = zio.cache.Cache.makeWithKey(config.cacheSize, zio.cache.Lookup((Registry.SttpRegistry.getOrRegisterId _).tupled))(
      _ => 1.hour,
      { case (_, _, _, subject, schema) => (subject, schema) }
    )
    val schemaIdToValidationCache =
      zio.cache.Cache.makeWithKey(config.cacheSize, zio.cache.Lookup((Registry.SttpRegistry.verifySchema _).tupled))(
        _ => 1.hour,
        { case (_, _, _, id, schema) => (id, schema) }
      )
    val log = log4sFromName.provideEnvironment(ZEnvironment("tamer.SttpRegistry"))
    (sttpBackend <*> schemaToIdCache <*> schemaIdToValidationCache <*> log)
      .mapError(TamerError("Cannot construct SttpRegistry client", _))
      .flatMap { case (backend, schemaToIdCache, schemaIdToValidationCache, log) =>
        ZIO.succeed(new Registry.SttpRegistry(backend, config.url, schemaToIdCache, schemaIdToValidationCache, log) {}) <*
          log.info(s"SttpRegistry client created successfully").ignore
      }
  }) {}
}

sealed trait LowPriorityRegistryProvider {
  implicit final def fakeRegistryProvider[A]: RegistryProvider[Any] = new RegistryProvider(_ => ZIO.succeed(Registry.FakeRegistry)) {}
}
