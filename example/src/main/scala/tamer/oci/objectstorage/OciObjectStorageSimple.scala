package tamer
package oci.objectstorage

import com.oracle.bmc.Region.US_PHOENIX_1
import zio._
import zio.duration._
import zio.oci.objectstorage._

object OciObjectStorageSimple extends App {
  case class ObjectsCursor(startAfter: Option[String], current: Option[String])

  val program: ZIO[ZEnv, TamerError, Unit] = ObjectStorageSetup(
    namespace = "namespace",
    bucketName = "bucketName",
    initialState = ObjectsCursor(None, None)
  )(
    recordKey = (oc, _: String) => oc,
    stateFold = {
      case (ObjectsCursor(_, _), next @ Some(_)) => UIO(ObjectsCursor(next, next))
      case (ObjectsCursor(s, Some(_)), None)     => UIO(ObjectsCursor(s, None))
      case (ObjectsCursor(s, None), None)        => ZIO.sleep(1.minute) *> UIO(ObjectsCursor(s, None))
    },
    objectName = _.current,
    startAfter = _.startAfter
  ).runWith(objectStorageLayer(US_PHOENIX_1, ObjectStorageAuth.fromConfigFileDefaultProfile) ++ kafkaConfigFromEnvironment)

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}
