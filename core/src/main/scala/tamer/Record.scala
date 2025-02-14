/*
 * Copyright (c) 2019-2025 LaserDisc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

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
  final case class Header(key: String, value: Array[Byte]) extends KHeader

  def apply[K, V](key: K, value: V): Record[K, V]                        = Record(key, value, None, Nil)
  def apply[K, V](key: K, value: V, timestamp: Instant): Record[K, V]    = Record(key, value, Some(timestamp.toEpochMilli()), Nil)
  def apply[K, V](key: K, value: V, headers: Seq[KHeader]): Record[K, V] = Record(key, value, None, headers)
  def apply[K, V](key: K, value: V, timestamp: Instant, headers: Seq[KHeader]): Record[K, V] =
    Record(key, value, Some(timestamp.toEpochMilli()), headers)
}
