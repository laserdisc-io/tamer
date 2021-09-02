package tamer

private[tamer] final case class TamerError(msg: String, cause: Throwable) extends RuntimeException(msg, cause)
private[tamer] object TamerError {
  final def apply(msg: String): TamerError = new TamerError(msg, null)
}
