package tamer

final case class TamerError(msg: String, cause: Throwable) extends RuntimeException(msg, cause)
object TamerError {
  final def apply(msg: String): TamerError = new TamerError(msg, null)

  final def fromThrowable(th: Throwable): TamerError = TamerError(th.getMessage, th)
}
