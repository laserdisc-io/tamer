package tamer.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import tamer.AvroCodec
import tamer.kafka.embedded.KafkaTest
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.kafka.admin._
import zio.kafka.consumer.Consumer.OffsetRetrieval
import zio.kafka.consumer._
import zio.kafka.consumer.diagnostics.Diagnostics
import zio.kafka.producer._
import zio.kafka.serde.{Deserializer, Serde, Serializer}

import java.util.UUID

object KafkaTestUtils {
  case class Key(key: Int)
  object Key {
    implicit val codec = AvroCodec.codec[Key]
  }
  case class Value(value: Int)
  object Value {
    implicit val codec = AvroCodec.codec[Value]
  }
  case class State(i: Int)
  object State {
    implicit val codec = AvroCodec.codec[State]
  }

  type StringProducer = Producer[Any, String, String]

  val producerSettings: ZIO[KafkaTest, Nothing, ProducerSettings] =
    ZIO.access[KafkaTest](_.get[KafkaTest.Service].bootstrapServers).map(hl => ProducerSettings(hl.value))

  val stringProducer: ZLayer[KafkaTest, Throwable, StringProducer] =
    (producerSettings.toLayer ++ ZLayer.succeed(Serde.string: Serializer[Any, String])) >>>
      Producer.live[Any, String, String]

  def produceOne(
      topic: String,
      key: String,
      message: String
  ): ZIO[Blocking with StringProducer, Throwable, RecordMetadata] =
    Producer.produce[Any, String, String](new ProducerRecord(topic, key, message))

  def produceMany(
      topic: String,
      partition: Int,
      kvs: Iterable[(String, String)]
  ): ZIO[Blocking with StringProducer, Throwable, Chunk[RecordMetadata]] =
    Producer
      .produceChunk[Any, String, String](Chunk.fromIterable(kvs.map { case (k, v) =>
        new ProducerRecord(topic, partition, null, k, v)
      }))

  def produceMany(
      topic: String,
      kvs: Iterable[(String, String)]
  ): ZIO[Blocking with StringProducer, Throwable, Chunk[RecordMetadata]] =
    Producer
      .produceChunk[Any, String, String](Chunk.fromIterable(kvs.map { case (k, v) =>
        new ProducerRecord(topic, k, v)
      }))

  def consumerSettings(groupId: String, clientId: String, offsetRetrieval: OffsetRetrieval = OffsetRetrieval.Auto()) =
    ZIO.access[KafkaTest] { kafka: KafkaTest =>
      ConsumerSettings(kafka.get.bootstrapServers.value)
        .withGroupId(groupId)
        .withClientId(clientId)
        .withCloseTimeout(5.seconds)
        .withProperties(
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG     -> "earliest",
          ConsumerConfig.METADATA_MAX_AGE_CONFIG      -> "100",
          ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG    -> "1000",
          ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG -> "250",
          ConsumerConfig.MAX_POLL_RECORDS_CONFIG      -> "10"
        )
        .withPerPartitionChunkPrefetch(16)
        .withOffsetRetrieval(offsetRetrieval)
    }

  def consumer(
      groupId: String,
      clientId: String,
      offsetRetrieval: OffsetRetrieval = OffsetRetrieval.Auto(),
      diagnostics: Diagnostics = Diagnostics.NoOp
  ): ZLayer[KafkaTest with Clock with Blocking, Throwable, Consumer] =
    (consumerSettings(groupId, clientId, offsetRetrieval).toLayer ++
      ZLayer.requires[Clock with Blocking] ++
      ZLayer.succeed(diagnostics)) >>> Consumer.live

  def consumeWithStrings[RC](groupId: String, clientId: String, subscription: Subscription)(
      r: (String, String) => URIO[RC, Unit]
  ): RIO[RC with Blocking with Clock with KafkaTest, Unit] =
    consumerSettings(groupId, clientId).flatMap { settings =>
      Consumer.consumeWith(
        settings,
        subscription,
        Deserializer.string,
        Deserializer.string
      )(r)
    }

  def adminSettings: ZIO[KafkaTest, Nothing, AdminClientSettings] =
    ZIO.access[KafkaTest](_.get[KafkaTest.Service].bootstrapServers).map(hl => AdminClientSettings(hl.value))

  def withAdmin[T](f: AdminClient => RIO[Clock with KafkaTest with Blocking, T]) =
    for {
      settings <- adminSettings
      fRes <- AdminClient
        .make(settings)
        .use(client => f(client))
        .provideSomeLayer[KafkaTest](Clock.live ++ Blocking.live)
    } yield fRes

  def randomThing(prefix: String): Task[String] =
    Task(UUID.randomUUID()).map(uuid => s"$prefix-$uuid")

  def randomTopic: Task[String] = randomThing("topic")

  def randomGroup: Task[String] = randomThing("group")

}
