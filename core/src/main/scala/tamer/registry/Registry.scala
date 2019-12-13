package tamer
package registry

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.{Schema, SchemaValidatorBuilder}
import zio.{RIO, Task}

import scala.collection.JavaConverters._

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
      private[this] final val strategy = new SchemaValidatorBuilder().canReadStrategy().validateLatest()
      private[this] final def validate[R](toValidate: Schema, writerSchema: Schema): RIO[R, Unit] =
        Task(strategy.validate(toValidate, List(writerSchema).asJava)).as(())
      override final def getOrRegisterId(subject: String, schema: Schema): Task[Int] =
        Task(client.getId(subject, schema)) orElse Task(client.register(subject, schema))
      override final def verifySchema(id: Int, schema: Schema): Task[Unit] =
        Task(client.getById(id)).flatMap(validate(schema, _))
    }
  }
}
