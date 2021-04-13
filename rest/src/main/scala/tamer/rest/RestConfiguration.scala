package tamer
package rest

import com.sksamuel.avro4s.Codec
import sttp.capabilities.zio.ZioStreams
import sttp.client3.{Identity, RequestT}
import sttp.model.Uri
import zio.stream.ZTransducer
import zio.{UIO, ZIO, stream, RIO}

trait RestQueryBuilder[-S] {

  /** Used for hashing purposes
    */
  val queryId: Int

  def query(state: S): Uri
}

final case class DecodedPage[V, S](data: List[V], stateSideband: Option[S])
final case class RestConfiguration[
    -R,
    K <: Product: Codec,
    V <: Product: Codec,
    S <: Product: Codec: HashableState
](
    queryBuilder: RestQueryBuilder[S],
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
  case class State[K, V, S](
      initialState: S,
      getNextState: S => UIO[S],
      deriveKafkaRecordKey: (S, V) => K
  )
  object State {
    def apply[K, V, S] = apply2[K, V, S] _
    def apply2[K, V, S](initialState: S)(getNextState: S => UIO[S], deriveKafkaRecordKey: (S, V) => K) =
      new State(initialState, getNextState, deriveKafkaRecordKey)
  }
}
