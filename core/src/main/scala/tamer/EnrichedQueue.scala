package tamer

import zio._

/** This class is needed due to the removal of contramap from ZQueue in 2.x.
  *
  * Being the underlying queue used a bounded one, we can always return an empty Chunk when we offer all (as it will block).
  *
  * @param underlying
  *   The underlying Enqueue[B] (bounded)
  * @param f
  *   The function used to emulate contramap
  * @see
  *   https://github.com/zio/zio/blob/6d42e56adc886cd7d43aaeff4b1a408fc4c19bd3/core/shared/src/main/scala/zio/ZQueue.scala#L216
  * @see
  *   https://github.com/zio/zio/blob/449ecc968de4f2d2a0b5ca9f8a8ff850fe204d6b/core/shared/src/main/scala/zio/Enqueue.scala#L46-L63
  */
class EnrichedBoundedEnqueue[-A, B](underlying: Enqueue[B], f: A => B) extends Enqueue[A] {
  override final def awaitShutdown(implicit trace: Trace): UIO[Unit]  = underlying.awaitShutdown
  override final def capacity: Int                                    = underlying.capacity
  override final def isShutdown(implicit trace: Trace): UIO[Boolean]  = underlying.isShutdown
  override final def offer(a: A)(implicit trace: Trace): UIO[Boolean] = ZIO.succeed(f(a)).flatMap(underlying.offer)
  override final def offerAll[A1 <: A](as: Iterable[A1])(implicit trace: Trace): UIO[Chunk[A1]] =
    ZIO.foreach(as)(a => ZIO.succeed(f(a))).flatMap(underlying.offerAll).as(Chunk.empty)
  override final def shutdown(implicit trace: Trace): UIO[Unit] = underlying.shutdown
  override final def size(implicit trace: Trace): UIO[Int]      = underlying.size
}
