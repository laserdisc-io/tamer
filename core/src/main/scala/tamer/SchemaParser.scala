package tamer

import scala.annotation.implicitNotFound

@implicitNotFound(
  "\n" +
    "Could not find or construct a \u001b[36mtamer.SchemaParser\u001b[0m instance for types:\n" +
    "\n" +
    "  \u001b[32m${S}\u001b[0m and \u001b[32m${PS}\u001b[0m\n" +
    "\n" +
    "There are a few provided OOTB, but consider creating one for the above types, if appropriate.\n"
)
trait SchemaParser[-S, +PS] {
  def parse(s: S): Option[PS]
}

object SchemaParser extends LowPrioritySchemaParser {
  final def apply[S, PS](implicit PS: SchemaParser[S, PS]): SchemaParser[S, PS] = PS

  implicit final val avroSchemaConfluentParsedSchemaParser: SchemaParser[org.apache.avro.Schema, io.confluent.kafka.schemaregistry.ParsedSchema] =
    new SchemaParser[org.apache.avro.Schema, io.confluent.kafka.schemaregistry.ParsedSchema] {
      override final def parse(s: org.apache.avro.Schema): Option[io.confluent.kafka.schemaregistry.ParsedSchema] =
        scala.util.Try(new io.confluent.kafka.schemaregistry.avro.AvroSchema(s)).toOption
    }
}
sealed trait LowPrioritySchemaParser {
  implicit final def identitySchemaParser[S]: SchemaParser[S, S] = new SchemaParser[S, S] {
    override final def parse(s: S): Option[S] = Some(s)
  }
}
