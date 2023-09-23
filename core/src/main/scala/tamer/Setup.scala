package tamer

import zio._

abstract class Setup[-R, K: Tag, V: Tag, SV: Tag](implicit val serdesProvider: SerdesProvider[K, V, SV]) {
  val initialState: SV
  val stateKey: Int
  val recordKey: (SV, V) => K
  val repr: String = "no repr string implemented, if you want a neat description of the source configuration please implement it"
  def iteration(currentState: SV, queue: Enqueue[NonEmptyChunk[(K, V)]]): RIO[R, SV]

  final val run: ZIO[R with KafkaConfig, TamerError, Unit] = runLoop.provideSomeLayer(Tamer.live(this))
  final def runWith[E >: TamerError, R1](layer: Layer[E, R with KafkaConfig with R1]): IO[E, Unit] =
    runLoop.provideLayer(layer >>> Tamer.live(this))
}
