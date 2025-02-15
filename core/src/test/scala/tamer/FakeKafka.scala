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

import io.github.embeddedkafka.schemaregistry.{EmbeddedKWithSR, EmbeddedKafka, EmbeddedKafkaConfig}
import kafka.server.UnboundedControllerMutationQuota
import zio._

trait FakeKafka {
  def bootstrapServers: List[String]
  def schemaRegistryUrl: String
  def createTopic(topic: String): Task[Unit]
  def stop(): UIO[Unit]
}

object FakeKafka {

  case class EmbeddedKafkaService(embeddedKWithSR: EmbeddedKWithSR) extends FakeKafka {
    private[this] final def _createTopic(topic: String) =
      embeddedKWithSR.broker.autoTopicCreationManager.createTopics(Set(topic), UnboundedControllerMutationQuota, None)

    override def bootstrapServers: List[String]         = List(s"localhost:${embeddedKWithSR.config.kafkaPort}")
    override def schemaRegistryUrl: String              = s"http://localhost:${embeddedKWithSR.config.schemaRegistryPort}"
    override def createTopic(topic: String): Task[Unit] = ZIO.attemptBlocking(_createTopic(topic)).unit
    override def stop(): UIO[Unit]                      = ZIO.attemptBlocking(embeddedKWithSR.stop(true)).ignore
  }

  case object DefaultLocal extends FakeKafka {
    override def bootstrapServers: List[String]         = List("localhost:9092")
    override def schemaRegistryUrl: String              = "http://localhost:8081"
    override def createTopic(topic: String): Task[Unit] = ZIO.unit
    override def stop(): UIO[Unit]                      = ZIO.unit
  }

  val kafkaConfigLayer: RLayer[FakeKafka, KafkaConfig] = ZLayer {
    for {
      randomString <- Random.nextUUID.map(uuid => s"test-$uuid")
      fakeKafka    <- ZIO.service[FakeKafka]
      _            <- fakeKafka.createTopic(s"sink.topic.$randomString")
      _            <- fakeKafka.createTopic(s"state.topic.$randomString")
    } yield KafkaConfig(
      brokers = fakeKafka.bootstrapServers,
      maybeRegistry = Some(RegistryConfig(fakeKafka.schemaRegistryUrl)),
      closeTimeout = 1.second,
      bufferSize = 5,
      sink = TopicConfig(s"sink.topic.$randomString"),
      state = TopicConfig(s"state.topic.$randomString"),
      groupId = s"groupid.$randomString",
      clientId = s"clientid.$randomString",
      transactionalId = s"transactionalid.$randomString"
    )
  }

  val embeddedKafkaLayer: TaskLayer[FakeKafka] = ZLayer.scoped {
    implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
      customBrokerProperties = Map("group.min.session.timeout.ms" -> "500", "group.initial.rebalance.delay.ms" -> "0")
    )
    ZIO.acquireRelease(ZIO.attempt(EmbeddedKafkaService(EmbeddedKafka.start())))(_.stop())
  }

  val embeddedKafkaConfigLayer: TaskLayer[KafkaConfig] = embeddedKafkaLayer >>> kafkaConfigLayer

  val localKafkaLayer: ULayer[FakeKafka] = ZLayer.succeed(DefaultLocal)
}
