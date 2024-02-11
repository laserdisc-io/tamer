package tamer

import java.lang.{Long => JLong}
import java.time.Instant

import org.apache.kafka.common.header.{Header => KHeader}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.jdk.CollectionConverters.IterableHasAsJava

final case class Record[K, V](key: K, value: V, timestamp: Option[Long], headers: Iterable[KHeader]) {
  def toKafkaProducerRecord(topic: String): ProducerRecord[K, V] =
    new ProducerRecord(topic, null, timestamp.map(JLong.valueOf).orNull, key, value, headers.asJava)
}

object Record {
  private final case class HeaderImpl private (key: String, value: Array[Byte]) extends KHeader
  object Header {
    def apply(key: String, value: Array[Byte]): KHeader = HeaderImpl(key, value)
  }

  def apply[K, V](key: K, value: V): Record[K, V]                        = Record(key, value, None, Nil)
  def apply[K, V](key: K, value: V, timestamp: Instant): Record[K, V]    = Record(key, value, Some(timestamp.toEpochMilli()), Nil)
  def apply[K, V](key: K, value: V, headers: Seq[KHeader]): Record[K, V] = Record(key, value, None, headers)
  def apply[K, V](key: K, value: V, timestamp: Instant, headers: Seq[KHeader]): Record[K, V] =
    Record(key, value, Some(timestamp.toEpochMilli()), headers)
}
