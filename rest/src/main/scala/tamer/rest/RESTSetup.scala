package tamer
package rest

import sttp.capabilities.zio.ZioStreams
import sttp.capabilities.{Effect, WebSockets}
import sttp.client3.Request
import zio.{RIO, Task, URIO}

trait QueryBuilder[-R, -S] {

  /** Used for hashing purposes
    */
  val queryId: Int

  def query(state: S): Request[Either[String, String], ZioStreams with Effect[Task] with WebSockets]

  val authentication: Option[Authentication[R]] = None
}

final case class DecodedPage[V, S](data: List[V], nextState: Option[S])
object DecodedPage {
  def fromString[R, V, S](decoder: String => RIO[R, List[V]]): String => RIO[R, DecodedPage[V, S]] =
    decoder.andThen(_.map(DecodedPage(_, None)))
}

final class RESTSetup[
    -R,
    K: Codec,
    V: Codec,
    S: Codec: Hashable
](val queryBuilder: QueryBuilder[R, S], val pageDecoder: String => RIO[R, DecodedPage[V, S]])(
    val transitions: RESTSetup.State[R, K, V, S]
) {
  private val keyId: Int = queryBuilder.queryId + Hashable[S].hash(transitions.initialState)
  private val repr: String =
    s"""
       |query:             ${queryBuilder.query(transitions.initialState)}
       |query id:          ${queryBuilder.queryId}
       |default state:     ${transitions.initialState}
       |default state id:  ${Hashable[S].hash(transitions.initialState)}
       |default state key: $keyId
       |""".stripMargin.stripLeading()

  val generic: Setup[K, V, S] = Setup(Setup.Serdes[K, V, S], transitions.initialState, keyId, repr)
}

object RESTSetup {
  def identityFilter[V, S](decodedPage: DecodedPage[V, S]): List[V] = decodedPage.data
  class State[-R, K, V, S](val initialState: S)(val deriveKafkaRecordKey: (S, V) => K)(
      val getNextState: (DecodedPage[V, S], S) => URIO[R, S],
      val filterPage: (DecodedPage[V, S], S) => List[V] = (decodedPage: DecodedPage[V, S], _: S) => identityFilter(decodedPage)
  )
}
