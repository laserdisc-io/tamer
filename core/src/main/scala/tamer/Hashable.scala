package tamer

trait Hashable[S] {

  /** It is required for this hash to be consistent even across executions
    * for the same semantic state. This is in contrast with the built-in
    * `hashCode` method.
    */
  def hash(s: S): Int
}

object Hashable {
  def apply[S](implicit h: Hashable[S]): Hashable[S] = h
}
