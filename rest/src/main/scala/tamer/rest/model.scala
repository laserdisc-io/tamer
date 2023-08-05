package tamer
package rest

import java.time.Instant

import zio._

import scala.annotation.unused

trait Authentication[-R] {
  def addAuthentication(request: SttpRequest, supplementalSecret: Option[String]): SttpRequest

  def setSecret(@unused secretRef: Ref[Option[String]]): RIO[R, Unit] = ZIO.unit
  // TODO: could return the value to set instead of unit

  def refreshSecret(secretRef: Ref[Option[String]]): RIO[R, Unit] = setSecret(secretRef)
}
object Authentication {
  def basic[R](username: String, password: String): Authentication[R] =
    (request: SttpRequest, _: Option[String]) => request.auth.basic(username, password)
}

final case class DecodedPage[V, S](data: List[V], nextState: Option[S])
object DecodedPage {
  def fromString[R, V, S](decoder: String => RIO[R, List[V]]): String => RIO[R, DecodedPage[V, S]] =
    decoder.andThen(_.map(DecodedPage(_, None)))
}

object EphemeralSecretCache {
  val live: ULayer[EphemeralSecretCache] = ZLayer(Ref.make[Option[String]](None))
}

case class Offset(offset: Int, nextIndex: Int) {
  def incrementedBy(increment: Int): Offset = this.copy(offset = offset + increment, nextIndex = 0)
  def nextIndex(index: Int): Offset         = this.copy(nextIndex = index)
}
object Offset {
  implicit final val hashable: Hashable[Offset] = s => s.offset * s.nextIndex
}

case class PeriodicOffset(offset: Int, periodStart: Instant) {
  def incrementedBy(increment: Int): PeriodicOffset = this.copy(offset = offset + increment)
}
object PeriodicOffset {
  implicit final val hashable: Hashable[PeriodicOffset] = s => s.offset * (s.periodStart.getEpochSecond + s.periodStart.getNano.longValue).toInt
}
