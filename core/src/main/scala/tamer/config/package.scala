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

  type DbConfig    = Has[Config.Db]
  type QueryConfig = Has[Config.Query]
  type KafkaConfig = Has[Config.Kafka]
  type TamerConfig = DbConfig with QueryConfig with KafkaConfig

  val dbConfig: URIO[DbConfig, Config.Db]          = ZIO.access(_.get)
  val queryConfig: URIO[QueryConfig, Config.Query] = ZIO.access(_.get)
  val kafkaConfig: URIO[KafkaConfig, Config.Kafka] = ZIO.access(_.get)
}
