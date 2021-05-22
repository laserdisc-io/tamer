package tamer

import eu.timepit.refined.api.Refined
import eu.timepit.refined.boolean.{And, Or}
import eu.timepit.refined.collection.{Forall, NonEmpty}
import eu.timepit.refined.string.{IPv4, Uri, Url}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{Has, URIO, ZIO}

package object kafka {
  type HostList  = List[String] Refined (NonEmpty And Forall[IPv4 Or Uri])
  type UrlString = String Refined Url

  final val config: URIO[Has[KafkaConfig], KafkaConfig]                         = ZIO.service
  final val runLoop: ZIO[Has[Kafka] with Blocking with Clock, TamerError, Unit] = ZIO.accessM(_.get.runLoop)
}
