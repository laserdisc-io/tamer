package tamer
package oci

import com.oracle.bmc.Region
import zio.{RIO, ZLayer}
import zio.oci.objectstorage._

package object objectstorage {
  final def objectStorageLayer[R](region: Region, auth: RIO[R, ObjectStorageAuth]): ZLayer[R, TamerError, ObjectStorage] =
    ZLayer(auth).flatMap(auth => ObjectStorage.live(ObjectStorageSettings(region, auth.get))).mapError { e =>
      TamerError(e.getLocalizedMessage, e)
    }
}
