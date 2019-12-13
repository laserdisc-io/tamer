package tamer

import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.boolean.{And, Or}
import eu.timepit.refined.collection.{Forall, MinSize, NonEmpty}
import eu.timepit.refined.string.{IPv4, Uri, Url}

package object config {
  type HostList  = List[String] Refined (NonEmpty And Forall[IPv4 Or Uri])
  type Password  = String Refined MinSize[W.`10`.T]
  type UriString = String Refined Uri
  type UrlString = String Refined Url
}
