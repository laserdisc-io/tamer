package tamer
package rest

import com.sksamuel.avro4s.Codec
import sttp.capabilities.zio.ZioStreams
import sttp.capabilities.{Effect, WebSockets}
import sttp.client3.Request
import zio.{RIO, Task, UIO}

trait RestQueryBuilder[-R, -S] {

  /** Used for hashing purposes
    */
  val queryId: Int

  def query(state: S): Request[Either[String, String], ZioStreams with Effect[Task] with WebSockets]

  val authentication: Option[Authentication[R]] = None
}

final case class DecodedPage[V, S](data: List[V], nextState: Option[S])
object DecodedPage {
  def fromString[R, V, S](decoder: String => RIO[R, List[V]]): String => RIO[R, DecodedPage[V, S]] =
    decoder.andThen(_.map(DecodedPage(_, Option.empty[S])))
}

final class RestConfiguration[
    -R,
    K: Codec,
    V: Codec,
    S: Codec: HashableState
](val queryBuilder: RestQueryBuilder[R, S], val pageDecoder: String => RIO[R, DecodedPage[V, S]])(
    val transitions: RestConfiguration.State[K, V, S]
) {
  private val keyId: Int = queryBuilder.queryId + HashableState[S].stateHash(transitions.initialState)
  private val repr: String =
    s"""
       |query:             ${queryBuilder.query(transitions.initialState)}
       |query id:          ${queryBuilder.queryId}
       |default state:     ${transitions.initialState}
       |default state id:  ${HashableState[S].stateHash(transitions.initialState)}
       |default state key: $keyId
       |""".stripMargin.stripLeading()

  val generic: SourceConfiguration[K, V, S] = SourceConfiguration(
    SourceConfiguration.SourceSerde[K, V, S](),
    transitions.initialState,
    keyId,
    repr
  )
}

object RestConfiguration {
  class State[K, V, S](val initialState: S)(
      val getNextState: S => UIO[S],
      val deriveKafkaRecordKey: (S, V) => K
  )
}
