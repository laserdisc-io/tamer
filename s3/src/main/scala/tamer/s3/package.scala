package tamer

import zio.Ref

package object s3 {
  type KeysR = Ref[Keys]
  type Keys  = List[String]
}
