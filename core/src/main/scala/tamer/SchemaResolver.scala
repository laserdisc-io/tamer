package tamer

import io.confluent.kafka.schemaregistry.ParsedSchema
import org.apache.avro.Schema

import scala.util.Try

trait SchemaResolver[S, T] {
  def resolve(s: S): Option[T]
}

object SchemaResolver {
  implicit final val avroSchemaConfluentResolver: SchemaResolver[Schema, ParsedSchema] = new SchemaResolver[Schema, ParsedSchema] {
    override final def resolve(s: Schema): Option[ParsedSchema] = Try(new io.confluent.kafka.schemaregistry.avro.AvroSchema(s)).toOption
  }
}
