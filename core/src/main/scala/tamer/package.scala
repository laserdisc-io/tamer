import zio.{RIO, ZIO}

package object tamer {
  final val runLoop: RIO[Tamer, Unit] = ZIO.serviceWithZIO(_.runLoop)

  implicit final class HashableOps[A](private val _underlying: A) extends AnyVal {
    def hash(implicit A: Hashable[A]): Int = A.hash(_underlying)
  }
}
