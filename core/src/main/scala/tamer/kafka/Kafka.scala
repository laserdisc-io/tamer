package tamer
package kafka

import java.security.MessageDigest

import eu.timepit.refined.auto._
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import tamer.config._
import tamer.registry._
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{CommittableRecord, Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.stream.ZStream

final case class StateKey(queryHash: String, groupId: String)

object Kafka {
  trait Service {
    def runLoop[K, V, State, R](setup: Setup[K, V, State])(
      f: (State, Queue[(K, V)]) => ZIO[R, TamerError, State]
    ): ZIO[R with Blocking with Clock, TamerError, Unit]
  }

  private[this] final val tamerErrors: PartialFunction[Throwable, TamerError] = {
    case ke: KafkaException => TamerError(ke.getLocalizedMessage, ke)
    case te: TamerError     => te
  }

  val live: URLayer[KafkaConfig, Kafka] = ZLayer.fromService { cfg =>
    new Service {
      private[this] val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.kafka")
      private[this] def stateKeyTask[A](a: A, s: String)(f: A => String): Task[StateKey] =
        Task(MessageDigest.getInstance("SHA-1")).map { md =>
          StateKey(md.digest(f(a).getBytes).take(7).map(b => f"$b%02x").mkString, s)
        }

      override final def runLoop[K, V, State, R](setup: Setup[K, V, State])(
        f: (State, Queue[(K, V)]) => ZIO[R, TamerError, State]
      ): ZIO[R with Blocking with Clock, TamerError, Unit] = {
        val registryTask            = Task(new CachedSchemaRegistryClient(cfg.schemaRegistryUrl, 4))
        val tenTimes                = Schedule.recurs(10) && Schedule.exponential(25.milliseconds)
        val offsetRetrievalStrategy = OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest)
        val cSettings               = ConsumerSettings(cfg.brokers).withGroupId(cfg.state.groupId).withClientId(cfg.state.clientId).withCloseTimeout(cfg.closeTimeout.zio).withOffsetRetrieval(offsetRetrievalStrategy)
        val pSettings               = ProducerSettings(cfg.brokers).withCloseTimeout(cfg.closeTimeout.zio)
        val stateTopicSub           = Subscription.topics(cfg.state.topic)
        val stateKeySerde           = Serde[StateKey](isKey = true)
        val stateConsumer           = Consumer.make(cSettings)
        val stateProducer           = Producer.make(pSettings, stateKeySerde.serializer, setup.stateSerde)
        val producer                = Producer.make(pSettings, setup.keySerializer, setup.valueSerializer)
        val queue                   = Managed.make(Queue.bounded[(K, V)](cfg.bufferSize))(_.shutdown)

        def mkRegistry(src: SchemaRegistryClient, topic: String) = (ZLayer.succeed(src) >>> Registry.live) ++ (ZLayer.succeed(topic) >>> Topic.live)

        def mkRecordChunk(kvs: List[(K, V)]) = Chunk.fromIterable(kvs.map { case (k, v) => new ProducerRecord(cfg.sink.topic, k, v) })
        def sink(q: Queue[(K, V)], p: Producer.Service[Registry with Topic, K, V], layer: ULayer[Registry with Topic]) =
          logTask.flatMap { log =>
            q.takeAll.flatMap {
              case Nil => log.trace("no data to push") *> ZIO.unit
              case kvs =>
                p.produceChunkAsync(mkRecordChunk(kvs)).provideSomeLayer[Blocking](layer).retry(tenTimes).flatten.unit <*
                  log.info(s"pushed ${kvs.size} messages to ${cfg.sink.topic}")
            }
          }

        def mkRecord(k: StateKey, v: State)      = new ProducerRecord(cfg.state.topic, k, v)
        def waitAssignment(sc: Consumer.Service) = sc.assignment.withFilter(_.nonEmpty).retry(tenTimes)
        def subscribe(sc: Consumer.Service)      = sc.subscribe(stateTopicSub) *> waitAssignment(sc).flatMap(sc.endOffsets(_)).map(_.values.exists(_ > 0L))
        def source(sc: Consumer.Service, q: Queue[(K, V)], sp: Producer.Service[Registry with Topic, StateKey, State], layer: ULayer[Registry with Topic]) =
          ZStream.fromEffect(logTask <*> stateKeyTask(setup.defaultState, cfg.state.groupId)(setup.buildQuery(_).sql)).flatMap { case (log, stateKey) =>
            ZStream
              .fromEffect(subscribe(sc))
              .flatMap {
                case true => ZStream.fromEffect(log.info(s"consumer group ${cfg.state.groupId} resuming consumption from ${cfg.state.topic}"))
                case false =>
                  ZStream.fromEffect {
                    log.info(s"consumer group ${cfg.state.groupId} never consumed from ${cfg.state.topic}, setting offset to earliest") *>
                      sp.produceAsync(mkRecord(stateKey, setup.defaultState))
                        .provideSomeLayer[Blocking](layer)
                        .flatten
                        .flatMap(rm => log.info(s"pushed initial state ${setup.defaultState} to $rm"))
                  }
              }
              .drain ++
              sc.plainStream(stateKeySerde.deserializer, setup.stateSerde)
                .provideSomeLayer[Blocking with Clock](layer)
                .mapM {
                  case CommittableRecord(record, offset) if record.key == stateKey =>
                    log.debug(s"consumer group ${cfg.state.groupId} consumed state ${record.value} from ${offset.topicPartition}@${offset.offset}") *>
                      f(record.value, q).flatMap { newState =>
                        sp.produceAsync(mkRecord(stateKey, newState))
                          .provideSomeLayer[Blocking](layer)
                          .flatten
                          .flatMap(rmd => log.debug(s"pushed state $newState to $rmd"))
                          .as(offset)
                      }
                  case CommittableRecord(_, offset) =>
                    log.debug(s"consumer group ${cfg.state.groupId} ignored state (wrong key) from ${offset.topicPartition}@${offset.offset}") *> UIO(offset)
                }
          }

        ZStream
          .fromEffect(logTask <&> registryTask)
          .flatMap { case (log, src) =>
            ZStream
              .managed(stateConsumer <&> stateProducer <&> producer <&> queue)
              .flatMap { case (((sc, sp), p), q) =>
                val sinkRegistry  = mkRegistry(src, cfg.sink.topic)
                val stateRegistry = mkRegistry(src, cfg.state.topic)
                ZStream.fromEffect(sink(q, p, sinkRegistry).forever.fork <* log.info("running sink perpetually")).flatMap { fiber =>
                  source(sc, q, sp, stateRegistry).ensuringFirst {
                    fiber.interrupt *> log.info("sink interrupted").ignore *> sink(q, p, sinkRegistry).run <* log.info("sink queue drained").ignore
                  }
                }
              }
              .aggregateAsync(Consumer.offsetBatches)
              .mapM(ob => ob.commit <* log.debug(s"consumer group ${cfg.state.groupId} committed offset batch ${ob.offsets}"))
          }
          .runDrain
          .refineOrDie(tamerErrors)
      }
    }
  }
}
