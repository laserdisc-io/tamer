package tamer

import zio._

abstract class Setup[-R, K: Tag, V: Tag, SV: Tag](implicit val serdesProvider: SerdesProvider[K, V, SV]) {
  val initialState: SV
  val stateKey: Int
  val repr: String = "no repr string implemented, if you want a neat description of the source configuration please implement it"
  def iteration(currentState: SV, queue: Enqueue[NonEmptyChunk[Record[K, V]]]): RIO[R, SV]

  final val run: RIO[R with KafkaConfig, Unit]                                    = runLoop.provideSomeLayer(Tamer.live(this)).orDie
  final def runWith[R1](layer: TaskLayer[R with KafkaConfig with R1]): Task[Unit] = runLoop.provideLayer(layer >>> Tamer.live(this)).orDie
}
