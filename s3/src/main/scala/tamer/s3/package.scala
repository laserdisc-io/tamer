package tamer

import zio._

package object s3 {
  type KeysR = Ref[List[String]]
  type Keys  = List[String]
}
