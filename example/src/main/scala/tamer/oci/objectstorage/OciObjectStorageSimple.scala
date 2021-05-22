package tamer
package oci.objectstorage

import com.oracle.bmc.Region
import tamer.kafka.KafkaConfig
import zio._
import zio.clock.Clock
import zio.duration.durationInt
import zio.oci.objectstorage._
import zio.stream.Transducer

object OciObjectStorageSimple extends App {
  case class ObjectsCursor(startAfter: Option[String], current: Option[String])
  object ObjectsCursor {
    implicit val codec = AvroCodec.codec[ObjectsCursor]
  }

  implicit final val stringCodec = AvroCodec.codec[String]

  val initialState = ObjectsCursor(None, None)

  private val transducer = Transducer.utf8Decode >>> Transducer.splitLines

  val objectNameBuilder = new ObjectNameBuilder[ObjectsCursor] {
    override def startAfter(state: ObjectsCursor): Option[String] = state.startAfter
    override def objectName(state: ObjectsCursor): Option[String] = state.current
  }

  def getNextState(oc: ObjectsCursor, next: Option[String]): URIO[Clock, ObjectsCursor] =
    (oc.startAfter, oc.current, next) match {
      case (_, _, Some(_))    => UIO(ObjectsCursor(next, next))
      case (s, Some(_), None) => UIO(ObjectsCursor(s, None))
      case (s, None, None)    => ZIO.sleep(1.minute) *> UIO(ObjectsCursor(s, None))
    }

  val program = ObjectStorageTamer(
    ObjectStorageSetup[ZEnv with ObjectStorage with Has[KafkaConfig], ObjectsCursor, String, ObjectsCursor](
      "namespace",
      "bucketName",
      None,
      _ => true,
      objectNameBuilder,
      ObjectStorageSetup.State(initialState, getNextState, (o: ObjectsCursor, _: String) => o),
      transducer
    )
  )

  val authLayer   = ZLayer.fromEffect(ObjectStorageAuth.fromConfigFileDefaultProfile)
  val osLayer     = ZLayer.fromServiceManaged((auth: ObjectStorageAuth) => live(ObjectStorageSettings(Region.US_PHOENIX_1, auth)).build.map(_.get))
  val osFullLayer = authLayer >>> osLayer

  val fullLayer = osFullLayer ++ KafkaConfig.fromEnvironment

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.run.provideCustomLayer(fullLayer).exitCode
}
