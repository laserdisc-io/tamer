package tamer.s3

import cats.implicits._
import ciris.refined.refTypeConfigDecoder
import ciris.{ConfigException, ConfigValue, env}
import eu.timepit.refined.types.numeric.PosInt
import tamer.TamerError
import zio.interop.catz.{taskConcurrentInstance, zioContextShift}
import zio.{Has, Layer, Task}

import scala.concurrent.duration.FiniteDuration

object ConfigS3 {
  type S3Configuration = Has[S3Config]

  final case class S3Config(
      minimumIntervalToFetchNewFiles: Option[FiniteDuration],
      keysPaginationMax: Option[PosInt]
  )

  private[this] val s3ConfigValue: ConfigValue[S3Config] = (
    env("MINIMUM_INTERVAL_TO_FETCH_NEW_FILES").as[FiniteDuration].option,
    env("KEYS_PAGINATION_MAX").as[PosInt].option
  ).parMapN(S3Config)

  val live: Layer[TamerError, S3Configuration] =
    s3ConfigValue.load[Task].refineToOrDie[ConfigException].mapError(ce => TamerError(ce.error.redacted.show, ce)).toLayer
}
