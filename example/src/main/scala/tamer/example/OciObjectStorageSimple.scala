package tamer.example

import com.oracle.bmc.Region
import tamer.{AvroCodec, TamerError}
import tamer.config.{Config, KafkaConfig}
import tamer.oci.objectstorage.{InternalState, LastProcessedFile, OciObjectStorageConfiguration, TamerOciObjectStorageJob}
import zio.{ExitCode, Has, Queue, Ref, UIO, URIO, ZEnv, ZIO, ZLayer}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.oci.objectstorage._
import zio.stream.ZTransducer

object OciObjectStorageSimple extends zio.App {
  case class MyLine(line: String)
  object MyLine {
    implicit val codec = AvroCodec.codec[MyLine]
  }
  private val transducer = ZTransducer.utf8Decode >>> ZTransducer.splitLines.map(MyLine.apply)

  def getNextState(is: Ref[InternalState], lpf: LastProcessedFile, token: Queue[Unit]): UIO[LastProcessedFile] = {
    val retry = token.take *> getNextState(is, lpf, token)
    is.get.flatMap(_.lastFileName match {
      case Some(value) => UIO(LastProcessedFile(Some(value)))
      case _           => retry
    })
  }

  private val transitions = OciObjectStorageConfiguration.State(LastProcessedFile(None), getNextState, (l: LastProcessedFile, _: MyLine) => l)

  val program: ZIO[Blocking with Clock with ObjectStorage with KafkaConfig, TamerError, Unit] = for {
    _ <- TamerOciObjectStorageJob[Blocking with Clock with ObjectStorage with KafkaConfig, LastProcessedFile, MyLine, LastProcessedFile](
      OciObjectStorageConfiguration("namespace", "bucketName", None, transducer, transitions)
    ).fetch()
  } yield ()

  val authLayer: ZLayer[Blocking, InvalidAuthDetails, Has[ObjectStorageAuth]] = ZLayer.fromEffect(ObjectStorageAuth.fromConfigFileDefaultProfile)

  private lazy val objectStorageLayer: ZLayer[Has[ObjectStorageAuth], Throwable, ObjectStorage] =
    ZLayer.fromServiceManaged(auth => live(ObjectStorageSettings(Region.US_PHOENIX_1, auth)).build.map(_.get))

  private lazy val fullObjectStorageLayer: ZLayer[Blocking, Throwable, ObjectStorage] = authLayer >>> objectStorageLayer

  private lazy val fullLayer = fullObjectStorageLayer ++ Config.live

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = program.provideCustomLayer(fullLayer).exitCode
}
