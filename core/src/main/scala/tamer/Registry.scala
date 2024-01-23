package tamer

import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import sttp.client4._
import sttp.client4.upicklejson.default._
import sttp.model._
import upickle.default._
import zio._
import zio.cache._

trait Registry {
  def getOrRegisterId(subject: String, schema: Schema): Task[Int]
  def verifySchema(id: Int, schema: Schema): Task[Unit]
}

object Registry {
  private[this] final type LoadingCache[K, R] = Cache[(Backend[Task], String, LogWriter[Task], K, Schema), Throwable, R]

  object SttpRegistry {
    private[this] final case class SchemaString(schema: String)
    private[this] object SchemaString {
      implicit val rw: ReadWriter[SchemaString] = macroRW[SchemaString]
    }
    private[this] final case class SubjectIdVersionSchemaString(subject: String, id: Int, version: Int, schema: String)
    private[this] object SubjectIdVersionSchemaString {
      implicit val rw: ReadWriter[SubjectIdVersionSchemaString] = macroRW[SubjectIdVersionSchemaString]
    }
    private[this] final case class Id(id: Int)
    private[this] object Id {
      implicit val rw: ReadWriter[Id] = macroRW[Id]
    }

    private[this] final val schemaRegistryV1MediaType = MediaType.unsafeParse("application/vnd.schemaregistry.v1+json")
    private[this] final val schemaRegistryMediaType   = MediaType.unsafeParse("application/vnd.schemaregistry+json")
    private[this] final val request = basicRequest.headers(
      Header.accept(schemaRegistryV1MediaType, schemaRegistryMediaType, MediaType.ApplicationJson),
      Header.contentType(schemaRegistryV1MediaType)
    )

    private[this] final def getId(backend: Backend[Task], url: String, log: LogWriter[Task], subject: String, schema: Schema): Task[Int] =
      request
        .post(uri"$url/subjects/$subject?normalize=false&deleted=false")
        .body(SchemaString(schema.show))
        .response(asJson[SubjectIdVersionSchemaString])
        .send(backend)
        .flatMap(response => ZIO.fromEither(response.body.map(_.id)))
        .tap(id => log.debug(s"retrieved existing writer schema id: $id"))
    private[this] final def register(backend: Backend[Task], url: String, log: LogWriter[Task], subject: String, schema: Schema): Task[Int] =
      request
        .post(uri"$url/subjects/$subject/versions?normalize=false")
        .body(SchemaString(schema.show))
        .response(asJson[Id])
        .send(backend)
        .flatMap(response => ZIO.fromEither(response.body.map(_.id)))
        .tap(id => log.info(s"registered with id $id new subject $subject writer schema ${schema.show}"))
    private[this] final def get(backend: Backend[Task], url: String, log: LogWriter[Task], id: Int): Task[String] =
      request
        .get(uri"$url/schemas/ids/$id?subject=")
        .response(asJson[SchemaString])
        .send(backend)
        .flatMap(response => ZIO.fromEither(response.body.map(_.schema)))
        .tap(_ => log.debug(s"retrieved writer schema id: $id"))
    private[this] final def verify(schema: Schema, writerSchema: String): Task[Unit] =
      ZIO
        .succeed(schema.isCompatible(writerSchema))
        .filterOrElseWith(_.isEmpty) { errors =>
          ZIO.fail(TamerError(s"Backwards incompatible schema: ${errors.mkString(", ")}"))
        }
        .unit

    final def getOrRegisterId(backend: Backend[Task], url: String, log: LogWriter[Task], subject: String, schema: Schema): Task[Int] =
      getId(backend, url, log, subject, schema) <> register(backend, url, log, subject, schema)
    final def verifySchema(backend: Backend[Task], url: String, log: LogWriter[Task], id: Int, schema: Schema): Task[Unit] =
      get(backend, url, log, id).flatMap(verify(schema, _)).unit
  }
  sealed abstract class SttpRegistry(
      backend: Backend[Task],
      url: String,
      schemaToIdCache: LoadingCache[String, Int],
      schemaIdToValidationCache: LoadingCache[Int, Unit],
      log: LogWriter[Task]
  ) extends Registry {
    override final def getOrRegisterId(subject: String, schema: Schema): Task[Int] =
      schemaToIdCache.get((backend, url, log, subject, schema))
    override final def verifySchema(id: Int, schema: Schema): Task[Unit] =
      schemaIdToValidationCache.get((backend, url, log, id, schema))
  }

  object FakeRegistry extends Registry {
    override final def getOrRegisterId(subject: String, schema: Schema): Task[Int] = ZIO.succeed(-1)
    override final def verifySchema(id: Int, schema: Schema): Task[Unit]           = ZIO.unit
  }

  private[tamer] final val fakeRegistryZIO: RIO[Scope, Registry] = ZIO.succeed(Registry.FakeRegistry)
}

final case class RegistryProvider(from: RegistryConfig => RIO[Scope, Registry])

object RegistryProvider {
  implicit final val defaultRegistryProvider: RegistryProvider = RegistryProvider { config =>
    val sttpBackend = sttp.client4.httpclient.zio.HttpClientZioBackend.scoped()
    val schemaToIdCache = Cache.makeWithKey(config.cacheSize, Lookup((Registry.SttpRegistry.getOrRegisterId _).tupled))(
      _ => 1.hour,
      { case (_, _, _, subject, schema) => (subject, schema) }
    )
    val schemaIdToValidationCache = Cache.makeWithKey(config.cacheSize, Lookup((Registry.SttpRegistry.verifySchema _).tupled))(
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
  }
}
