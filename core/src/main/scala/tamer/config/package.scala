package tamer

import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.boolean.{And, Or}
import eu.timepit.refined.collection.{Forall, MinSize, NonEmpty}
import eu.timepit.refined.string.{IPv4, Uri}
import zio.ZIO

package object config extends Config.Service[Config] {
  type HostList  = List[String] Refined (NonEmpty And Forall[IPv4 Or Uri])
  type Password  = String Refined MinSize[W.`10`.T]
  type UriString = String Refined Uri

  override final val load: ZIO[Config, ConfigError, TamerConfig] = ZIO.accessM(_.config.load)
}
