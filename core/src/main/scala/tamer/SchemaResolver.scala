package tamer

import scala.annotation.implicitNotFound

@implicitNotFound(
  "\n" +
    "Could not find or construct a \u001b[36mtamer.SchemaResolver\u001b[0m instance for types:\n" +
    "\n" +
    "  \u001b[32m${S}\u001b[0m and \u001b[32m${RS}\u001b[0m\n" +
    "\n" +
    "There are a few provided OOTB, but consider creating one for the above types, if appropriate.\n"
)
trait SchemaResolver[-S, +RS] {
  def resolve(s: S): Option[RS]
}

object SchemaResolver extends LowPrioritySchemaResolver {
  implicit final val avroSchemaConfluentParsedSchemaResolver: SchemaResolver[org.apache.avro.Schema, io.confluent.kafka.schemaregistry.ParsedSchema] =
    new SchemaResolver[org.apache.avro.Schema, io.confluent.kafka.schemaregistry.ParsedSchema] {
      override final def resolve(s: org.apache.avro.Schema): Option[io.confluent.kafka.schemaregistry.ParsedSchema] =
        scala.util.Try(new io.confluent.kafka.schemaregistry.avro.AvroSchema(s)).toOption
    }
}
sealed trait LowPrioritySchemaResolver {
  implicit final def identityResolver[S]: SchemaResolver[S, S] = new SchemaResolver[S, S] {
    override final def resolve(s: S): Option[S] = Some(s)
  }
}
