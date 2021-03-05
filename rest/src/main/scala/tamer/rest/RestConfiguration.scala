package tamer
package rest

import com.sksamuel.avro4s.Codec
import sttp.capabilities.zio.ZioStreams
import sttp.client3.{Identity, RequestT}
import zio.stream.ZTransducer
import zio.{UIO, stream}

trait RestQueryBuilder[-S] {

  /** Used for hashing purposes
    */
  val queryId: Int

  def query(state: S): RequestT[Identity, Either[String, stream.Stream[Throwable, Byte]], ZioStreams]
}

final case class RestConfiguration[
    R,
    K <: Product: Codec,
    V <: Product: Codec,
    S <: Product: Codec: HashableState
](
    queryBuilder: RestQueryBuilder[S],
    transducer: ZTransducer[R, TamerError, Byte, V],
    transitions: RestConfiguration.State[K, V, S],
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
                             deriveKafkaRecordKey: (S, V) => K,
                           )


   def test: RequestT[Identity, Either[String, stream.Stream[Throwable, Byte]], ZioStreams] = {
     import sttp.capabilities.zio.ZioStreams
     import sttp.client3.{Identity, RequestT, _}
     import zio.stream._

     val request: RequestT[Identity, Either[String, Stream[Throwable, Byte]], ZioStreams] =
       basicRequest
         .post(uri"...")
         .response(asStreamUnsafe(ZioStreams))
         .readTimeout(scala.concurrent.duration.Duration.Inf)

     request
   }


}
