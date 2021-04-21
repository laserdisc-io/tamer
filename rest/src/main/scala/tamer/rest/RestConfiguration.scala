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

final case class RestConfiguration[
    -R,
    K: Codec,
    V: Codec,
    S: Codec: HashableState
](
    queryBuilder: RestQueryBuilder[R, S],
    pageDecoder: String => RIO[R, DecodedPage[V, S]],
    transitions: RestConfiguration.State[K, V, S]
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
  def apply[
      R,
      K,
      V,
      S
  ](queryBuilder: RestQueryBuilder[R, S])(
      pageDecoder: String => RIO[R, DecodedPage[V, S]]
  )(transitions: State[K, V, S])(implicit k: Codec[K], v: Codec[V], s1: Codec[S], s2: HashableState[S], d: DummyImplicit) =
    new RestConfiguration(queryBuilder, pageDecoder, transitions)

  case class State[K, V, S](
      initialState: S,
      getNextState: S => UIO[S],
      deriveKafkaRecordKey: (S, V) => K
  )
  object State {
    def apply[K, V, S](initialState: S)(getNextState: S => UIO[S], deriveKafkaRecordKey: (S, V) => K)(implicit d: DummyImplicit) =
      new State(initialState, getNextState, deriveKafkaRecordKey)
  }
}
