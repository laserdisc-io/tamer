package tamer

import scala.util.hashing._
import scala.util.hashing.MurmurHash3._
import java.time.Instant

trait Hashable[S] {

  /** It is required for this hash to be consistent even across executions for the same semantic state. This is in contrast with the built-in
    * `hashCode` method.
    */
  def hash(s: S): Int
}

object Hashable extends HashableInstances0 {
  def apply[S](implicit h: Hashable[S]): Hashable[S] = h
}

sealed trait HashableInstances0 extends HashableInstances1 {
  implicit final val instantHashable: Hashable[Instant] = i => byteswap64(i.getEpochSecond()).toInt
  implicit final val stringHashable: Hashable[String]   = stringHash(_)
}

sealed trait HashableInstances1 {
  implicit final def default[A](implicit A: Hashing[A]): Hashable[A] = A.hash(_)
}
