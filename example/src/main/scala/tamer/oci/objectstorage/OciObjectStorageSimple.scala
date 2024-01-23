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
    recordKey = (oc, _: String) => oc,
    stateFold = {
      case (ObjectsCursor(_, _), next @ Some(_)) => ZIO.succeed(ObjectsCursor(next, next))
      case (ObjectsCursor(s, Some(_)), None)     => ZIO.succeed(ObjectsCursor(s, None))
      case (ObjectsCursor(s, None), None)        => ZIO.sleep(1.minute) *> ZIO.succeed(ObjectsCursor(s, None))
    },
    objectName = _.current,
    startAfter = _.startAfter
  ).runWith(objectStorageLayer(US_PHOENIX_1, ObjectStorageAuth.fromConfigFileDefaultProfile) ++ KafkaConfig.fromEnvironment)
}
