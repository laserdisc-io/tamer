package tamer

import eu.timepit.refined.api.Refined
import eu.timepit.refined.boolean.{And, Or}
import eu.timepit.refined.collection.{Forall, NonEmpty}
import eu.timepit.refined.string.{IPv4, Uri, Url}
import zio.{Has, URIO, ZIO}

package object config {
  type HostList  = List[String] Refined (NonEmpty And Forall[IPv4 Or Uri])
  type Password  = String
  type UriString = String Refined Uri
  type UrlString = String Refined Url

  type KafkaConfig = Has[Config.Kafka]

  val kafkaConfig: URIO[KafkaConfig, Config.Kafka] = ZIO.access(_.get)
}
