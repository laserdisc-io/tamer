import zio.duration.Duration

import scala.concurrent.duration.FiniteDuration

package object tamer {
  implicit final class ScalaFiniteDurationToZIO(private val fd: FiniteDuration) extends AnyVal {
    final def zio: Duration = Duration.fromScala(fd)
  }
}
