import zio.duration.Duration

import scala.concurrent.duration.FiniteDuration

package object tamer {
  final type ZSerde[-R, T] = zio.kafka.client.serde.Serde[R, T]
  final val ZSerde = zio.kafka.client.serde.Serde

  implicit final class ScalaFiniteDurationToZIO(private val fd: FiniteDuration) extends AnyVal {
    final def zio: Duration = Duration.fromScala(fd)
  }
}
