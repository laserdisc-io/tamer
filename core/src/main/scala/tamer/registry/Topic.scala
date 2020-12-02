package tamer
package registry

import zio.{Has, URLayer, ZLayer}

object Topic {
  trait Service {
    def topic: String
  }

  val live: URLayer[Has[String], Topic] = ZLayer.fromService { t =>
    new Service {
      override final val topic: String = t
    }
  }
}
