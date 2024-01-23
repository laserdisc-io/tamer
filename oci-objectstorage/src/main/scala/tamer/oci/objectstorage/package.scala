package tamer
package oci

import com.oracle.bmc.Region
import zio.{RIO, RLayer, ZLayer}
import zio.oci.objectstorage._

package object objectstorage {
  final def objectStorageLayer[R](region: Region, auth: RIO[R, ObjectStorageAuth]): RLayer[R, ObjectStorage] =
    ZLayer(auth).flatMap(auth => ObjectStorage.live(ObjectStorageSettings(region, auth.get))).mapError { e =>
      TamerError(e.getLocalizedMessage, e)
    }
}
