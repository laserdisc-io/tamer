package tamer.example

import com.oracle.bmc.Region
import tamer.{AvroCodec, TamerError}
import tamer.config.{Config, KafkaConfig}
import tamer.oci.objectstorage.{ObjectNameBuilder, OciObjectStorageConfiguration, TamerOciObjectStorageJob}
import zio.{ExitCode, Has, UIO, URIO, ZEnv, ZIO, ZLayer}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.durationInt
import zio.oci.objectstorage._
import zio.stream.ZTransducer

object OciObjectStorageSimple extends zio.App {
  case class ObjectsCursor(startAfter: Option[String], current: Option[String])
  object ObjectsCursor {
    implicit val codec = AvroCodec.codec[ObjectsCursor]
  }

  case class MyLine(line: String)
  object MyLine {
    implicit val codec = AvroCodec.codec[MyLine]
  }

  val initialState = ObjectsCursor(None, None)

  private val transducer = ZTransducer.utf8Decode >>> ZTransducer.splitLines.map(MyLine.apply)

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

  val programs: ZIO[ZEnv with ObjectStorage with KafkaConfig, TamerError, Unit] = TamerOciObjectStorageJob(
    OciObjectStorageConfiguration[ZEnv with ObjectStorage with KafkaConfig, ObjectsCursor, MyLine, ObjectsCursor](
      "namespace",
      "bucketName",
      None,
      _ => true,
      objectNameBuilder,
      OciObjectStorageConfiguration.State(initialState, getNextState, (o: ObjectsCursor, _: MyLine) => o),
      transducer
    )
  ).fetch()

  val authLayer: ZLayer[Blocking, InvalidAuthDetails, Has[ObjectStorageAuth]] = ZLayer.fromEffect(ObjectStorageAuth.fromConfigFileDefaultProfile)
  val osLayer: ZLayer[Has[ObjectStorageAuth], ConnectionError, ObjectStorage] =
    ZLayer.fromServiceManaged(auth => live(ObjectStorageSettings(Region.US_PHOENIX_1, auth)).build.map(_.get))
  val osFullLayer: ZLayer[Blocking, Throwable, ObjectStorage] = authLayer >>> osLayer

  val fullLayer = osFullLayer ++ Config.live

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = programs.provideCustomLayer(fullLayer).exitCode
}
