/*
 * Copyright (c) 2019-2025 LaserDisc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package tamer
package oci.objectstorage

import com.oracle.bmc.Region.US_PHOENIX_1
import zio._

import zio.oci.objectstorage._

object OciObjectStorageSimple extends ZIOAppDefault {
  import implicits._

  override final val run = ObjectStorageSetup(
    namespace = "namespace",
    bucket = "bucketName",
    initialState = ObjectsCursor(None, None)
  )(
    recordFrom = (oc, v: String) => Record(oc, v),
    stateFold = {
      case (ObjectsCursor(_, _), next @ Some(_)) => ZIO.succeed(ObjectsCursor(next, next))
      case (ObjectsCursor(s, Some(_)), None)     => ZIO.succeed(ObjectsCursor(s, None))
      case (ObjectsCursor(s, None), None)        => ZIO.sleep(1.minute) *> ZIO.succeed(ObjectsCursor(s, None))
    },
    objectName = _.current,
    startAfter = _.startAfter
  ).runWith(objectStorageLayer(US_PHOENIX_1, ObjectStorageAuth.fromConfigFileDefaultProfile) ++ KafkaConfig.fromEnvironment)
}
