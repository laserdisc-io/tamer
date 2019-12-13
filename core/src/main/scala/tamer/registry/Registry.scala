package tamer
package registry

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.{Schema, SchemaValidatorBuilder}
import zio.{RIO, Task}

import scala.collection.JavaConverters._

trait Topic extends Any { def topic: String }
object Topic {
  sealed abstract class DefaultTopic(override final val topic: String) extends Topic

  final def apply(topic: String): Topic       = new DefaultTopic(topic) {}
  final def unapply(s: Topic): Option[String] = Some(s.topic)
}

trait TopicAndSchema {
  def topic: Topic
  def schema: Schema
}

object TopicAndSchema {
  sealed abstract class DefaultTopicAndSchema(override final val topic: Topic, override final val schema: Schema) extends TopicAndSchema

  final def apply(topic: String, schema: Schema): TopicAndSchema  = new DefaultTopicAndSchema(Topic(topic), schema) {}
  final def unapply(tas: TopicAndSchema): Option[(Topic, Schema)] = Some(tas.topic -> tas.schema)
}

trait Registry extends Serializable {
  val registry: Registry.Service[Any]
}

object Registry {
  trait Service[R] {
    def getOrRegisterId: RIO[R with TopicAndSchema, Int]
    def verifySchema(id: Int): RIO[R with TopicAndSchema, Unit]
  }

  object > extends Service[Registry] {
    override final val getOrRegisterId: RIO[Registry with TopicAndSchema, Int]        = RIO.accessM(_.registry.getOrRegisterId)
    override final def verifySchema(id: Int): RIO[Registry with TopicAndSchema, Unit] = RIO.accessM(_.registry.verifySchema(id))
  }

  trait Live extends Registry {
    val client: SchemaRegistryClient
    override final val registry: Service[Any] = new Service[Any] {
      private[this] final val strategy = new SchemaValidatorBuilder().canReadStrategy().validateLatest()
      private[this] final def validate[R](toValidate: Schema, writerSchema: Schema): RIO[R, Unit] =
        Task(strategy.validate(toValidate, List(writerSchema).asJava)).as(())
      override final val getOrRegisterId: RIO[TopicAndSchema, Int] = RIO.accessM {
        case TopicAndSchema(Topic(subject), schema) => Task(client.getId(subject, schema)) orElse Task(client.register(subject, schema))
      }
      override final def verifySchema(id: Int): RIO[TopicAndSchema, Unit] = RIO.accessM {
        case TopicAndSchema(_, schema) => RIO(client.getById(id)).flatMap(validate(schema, _))
      }
    }
  }
}
