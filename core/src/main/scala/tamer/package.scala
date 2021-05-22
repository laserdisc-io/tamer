import eu.timepit.refined.api.Refined
import eu.timepit.refined.boolean.{And, Or}
import eu.timepit.refined.collection.{Forall, NonEmpty}
import eu.timepit.refined.string.{IPv4, Uri, Url}
import zio.{Has, URIO, ZIO}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.Duration

import scala.concurrent.duration.FiniteDuration

package object tamer {
  final type HostList     = List[String] Refined (NonEmpty And Forall[IPv4 Or Uri])
  final type RegistryInfo = Has[Registry] with Has[TopicName]
  final type TopicName    = String
  final type UrlString    = String Refined Url

  final val kafkaConfig: URIO[Has[KafkaConfig], KafkaConfig]                    = ZIO.service
  final val runLoop: ZIO[Has[Tamer] with Blocking with Clock, TamerError, Unit] = ZIO.accessM(_.get.runLoop)

  final type ZSerde[-R, T] = zio.kafka.serde.Serde[R, T]
  final val ZSerde = zio.kafka.serde.Serde

  implicit final class ScalaFiniteDurationToZIO(private val _underlying: FiniteDuration) extends AnyVal {
    final def zio: Duration = Duration.fromScala(_underlying)
  }
}
