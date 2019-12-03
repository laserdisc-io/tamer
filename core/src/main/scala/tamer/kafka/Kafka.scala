package tamer
package kafka

import eu.timepit.refined.auto._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import tamer.config.KafkaConfig
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
        kc: KafkaConfig,
        setup: Setup[K, V, State]
    )(
        f: (State, Queue[(K, V)]) => ZIO[R0, E1, Unit]
    ): ZIO[R with R0 with Blocking with Clock, TamerError, Unit]
  }

  private[this] final val tamerErrors: PartialFunction[Throwable, TamerError] = {
    case ke: KafkaException => KafkaError(ke.getLocalizedMessage)
    case te: TamerError     => te
  }

  trait Live extends Kafka {
    override final val kafka: Kafka.Service[Any] = new Kafka.Service[Any] {
      override final def run[K, V, State, R0, E1 <: TamerError](
          kc: KafkaConfig,
          setup: Setup[K, V, State]
      )(
          f: (State, Queue[(K, V)]) => ZIO[R0, E1, Unit]
      ): ZIO[R0 with Blocking with Clock, TamerError, Unit] = {
        val tenTimesExponential = Schedule.recurs(10) && Schedule.exponential(25.milliseconds)
        val foreverFixed        = Schedule.fixed(3.minutes)
        val cSettings           = ConsumerSettings(kc.brokers, kc.state.groupId, kc.state.clientId, kc.closeTimeout.zio, Map.empty, 250.millis, 50.millis, 2)
        val pSettings           = ProducerSettings(kc.brokers, kc.closeTimeout.zio, Map.empty)
        val stateKey            = StateKey("FIXME", kc.state.groupId)
        val stateKeySerde       = Serdes[StateKey].serde
        val stateConsumer       = Consumer.make(cSettings)
        val stateProducer       = Producer.make(pSettings, stateKeySerde, setup.stateSerde)
        val producer            = Producer.make(pSettings, setup.keySerializer, setup.valueSerializer)
        val queue               = Managed.make(Queue.bounded[(K, V)](kc.bufferSize))(_.shutdown)

        def waitForAssignment(sc: Consumer) = sc.assignment.withFilter(_.nonEmpty).retry(tenTimesExponential)
        def sink(q: Queue[(K, V)], p: Producer[Any, K, V], sp: Producer[Any, StateKey, State], currentStateRef: Ref[State]) = {
          def processBatch(currentState: State)(b: List[(K, V)]) = b match {
            case Nil => ZIO.succeed(0 -> currentState)
            case batch =>
              val newS = setup.stateFold(currentState)(batch.map(_._2))
              val prs  = batch.map { case (k, v) => new ProducerRecord(kc.sink.topic, k, v) }
              val spr  = new ProducerRecord(kc.state.topic, stateKey, newS)
              p.produceChunk(Chunk.fromIterable(prs))
                .retry(tenTimesExponential)
                .flatten
                .flatMap(_ => sp.produce(spr).retry(tenTimesExponential).flatten)
                .as(batch.size -> newS)
          }
          for {
            currentState          <- currentStateRef.get
            (batchSize, newState) <- q.takeUpTo(kc.bufferSize).flatMap(processBatch(currentState))
            _                     <- currentStateRef.set(newState)
          } yield batchSize
        }
        def subscribeAndCheckOffsets(sc: Consumer) =
          sc.subscribe(Subscription.topics(kc.state.topic)) *> waitForAssignment(sc).flatMap(sc.endOffsets(_)).map(_.values.exists(_ > 0L))
        def consumeWith(sc: Consumer, q: Queue[(K, V)]) =
          sc.plainStream(stateKeySerde, setup.stateSerde)
            .mapM { case CommittableRecord(record, offset) => f(record.value, q).retry(foreverFixed).as(offset) }
            .flattenChunks
        def source(sc: Consumer, q: Queue[(K, V)]) =
          ZStream.fromEffect(subscribeAndCheckOffsets(sc)).flatMap {
            case true => ZStream.empty
            case false =>
              ZStream.fromEffect(f(setup.defaultState, q).retry(foreverFixed) &> waitForAssignment(sc).flatMap(sc.seekToBeginning(_))).drain
          } ++ consumeWith(sc, q)

        ZStream
          .managed(stateConsumer <&> stateProducer <&> producer <&> queue)
          .flatMap {
            case (((sc, sp), p), q) =>
              ZStream
                .fromEffect(Ref.make(setup.defaultState).flatMap(sink(q, p, sp, _).forever.fork))
                .flatMap(fiber => source(sc, q).ensuringFirst(fiber.interrupt))
          }
          .aggregateAsync(Consumer.offsetBatches)
          .mapM(_.commit)
          .runDrain
          .refineOrDie(tamerErrors)
      }
    }
  }
}
