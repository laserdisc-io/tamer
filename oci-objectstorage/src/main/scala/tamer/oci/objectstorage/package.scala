package tamer
package oci

import com.oracle.bmc.Region
import zio.{RIO, ZLayer}
import zio.oci.objectstorage._

package object objectstorage {
  final def objectStorageLayer[R](region: Region, auth: RIO[R, ObjectStorageAuth]): ZLayer[R, TamerError, ObjectStorage.Service] =
    (ZLayer.fromZIO(auth) >>> ZLayer.fromServiceManaged(auth => live(ObjectStorageSettings(region, auth)).build.map(_.get))).mapError { e =>
      TamerError(e.getLocalizedMessage, e)
    }
}
