package tamer
package kafka

import java.security.MessageDigest

import eu.timepit.refined.auto._
import log.effect.LogWriter
import log.effect.zio.ZioLogWriter.log4sFromName
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import tamer.config._
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.kafka.client._
import zio.kafka.client.serde._
import zio.stream.ZStream

final case class StateKey(table: String, groupId: String)

trait Kafka extends Serializable {
  val kafka: Kafka.Service[Any]
}

object Kafka {
  trait Service[R] {
    def run[K, V, State, R0, E1 <: TamerError](
        kafkaConfig: KafkaConfig,
        setup: Setup[K, V, State]
    )(
        f: (State, Queue[(K, V)]) => ZIO[R0, E1, State]
    ): ZIO[R with R0 with Blocking with Clock, TamerError, Unit]
  }

  private[this] final val tamerErrors: PartialFunction[Throwable, TamerError] = {
    case ke: KafkaException => KafkaError(ke.getLocalizedMessage)
    case te: TamerError     => te
  }

  trait Live extends Kafka {
    override final val kafka: Kafka.Service[Any] = new Kafka.Service[Any] {
      private[this] val logTask: Task[LogWriter[Task]] = log4sFromName.provide("tamer.Kafka.Live")
      private[this] def stateKeyTask[A](a: A, s: String)(f: A => String): Task[StateKey] = Task(MessageDigest.getInstance("SHA-1")).map { md =>
        StateKey(md.digest(f(a).getBytes).take(7).map(b => f"$b%02x").mkString, s)
      }

      override final def run[K, V, State, R0, E1 <: TamerError](
          kafkaConfig: KafkaConfig,
          setup: Setup[K, V, State]
      )(
          f: (State, Queue[(K, V)]) => ZIO[R0, E1, State]
      ): ZIO[R0 with Blocking with Clock, TamerError, Unit] = {
        val KafkaConfig(brokers, closeTimeout, bufferSize, sinkConfig, stateConfig) = kafkaConfig
        val KafkaSinkConfig(sinkTopic)                                              = sinkConfig
        val KafkaStateConfig(stateTopic, groupId, clientId)                         = stateConfig
        val Setup(keySer, valueSer, stateSerde, _, defaultState, buildQuery, _)     = setup

        val tenTimes      = Schedule.recurs(10) && Schedule.exponential(25.milliseconds)
        val cSettings     = ConsumerSettings(brokers, groupId, clientId, closeTimeout.zio, Map.empty, 250.millis, 50.millis, 2)
        val pSettings     = ProducerSettings(brokers, closeTimeout.zio, Map.empty)
        val stateTopicSub = Subscription.topics(stateTopic)
        val stateKeySerde = Serdes[StateKey].serde
        val stateConsumer = Consumer.make(cSettings)
        val stateProducer = Producer.make(pSettings, stateKeySerde, stateSerde)
        val producer      = Producer.make(pSettings, keySer, valueSer)
        val queue         = Managed.make(Queue.bounded[(K, V)](bufferSize))(_.shutdown)

        def mkRecordChunk(kvs: List[(K, V)]) = Chunk.fromIterable(kvs.map { case (k, v) => new ProducerRecord(sinkTopic, k, v) })
        def sink(q: Queue[(K, V)], p: Producer[Any, K, V]) = logTask.flatMap { log =>
          q.takeAll.flatMap {
            case Nil => log.debug("no data to push") *> ZIO.unit
            case kvs => p.produceChunk(mkRecordChunk(kvs)).retry(tenTimes).flatten.unit <* log.info(s"pushed ${kvs.size} messages to $sinkTopic")
          }
        }

        def mkRecord(k: StateKey, v: State) = new ProducerRecord(stateTopic, k, v)
        def waitAssignment(sc: Consumer)    = sc.assignment.withFilter(_.nonEmpty).retry(tenTimes)
        def subscribe(sc: Consumer)         = sc.subscribe(stateTopicSub) *> waitAssignment(sc).flatMap(sc.endOffsets(_)).map(_.values.exists(_ > 0L))
        def source(sc: Consumer, q: Queue[(K, V)], sp: Producer[Any, StateKey, State]) =
          ZStream.fromEffect(logTask <*> stateKeyTask(defaultState, groupId)(buildQuery(_).sql)).flatMap {
            case (log, stateKey) =>
              ZStream
                .fromEffect(subscribe(sc))
                .flatMap {
                  case true => ZStream.fromEffect(log.info(s"consumer group $groupId resuming consumption from $stateTopic"))
                  case false =>
                    ZStream.fromEffect(
                      {
                        log.info(s"consumer group $groupId never consumed from $stateTopic, setting offset to earliest") *>
                          sp.produce(mkRecord(stateKey, defaultState)).flatten.flatMap(rm => log.info(s"pushed initial state $defaultState to $rm"))
                      } &> waitAssignment(sc).flatMap { tps =>
                        sc.seekToBeginning(tps) *> log.info(s"consumer group $groupId assigned to $tps and offset now set to earliest")
                      }
                    )
                }
                .drain ++
                sc.plainStream(stateKeySerde, stateSerde)
                  .mapM {
                    case CommittableRecord(record, offset) if record.key == stateKey =>
                      log.debug(s"consumer group $groupId consumed state ${record.value} from ${offset.topicPartition}@${offset.offset}") *>
                        f(record.value, q).flatMap { newState =>
                          sp.produce(mkRecord(stateKey, newState)).flatten.flatMap(rmd => log.info(s"pushed state $newState to $rmd")).as(offset)
                        }
                    case CommittableRecord(_, offset) =>
                      log.debug(s"consumer group $groupId ignored state (wrong key) from ${offset.topicPartition}@${offset.offset}") *> UIO(offset)
                  }
                  .flattenChunks
          }

        ZStream
          .fromEffect(logTask)
          .flatMap { log =>
            ZStream
              .managed(stateConsumer <&> stateProducer <&> producer <&> queue)
              .flatMap {
                case (((sc, sp), p), q) =>
                  ZStream.fromEffect(sink(q, p).forever.fork <* log.info("running sink perpetually")).flatMap { fiber =>
                    source(sc, q, sp).ensuringFirst {
                      fiber.interrupt *> log.info("sink interrupted").ignore *> sink(q, p).run <* log.info("sink queue drained").ignore
                    }
                  }
              }
              .aggregateAsync(Consumer.offsetBatches)
              .mapM(ob => ob.commit <* log.debug(s"consumer group $groupId committed offset batch ${ob.offsets}"))
          }
          .runDrain
          .refineOrDie(tamerErrors)
      }
    }
  }
}
