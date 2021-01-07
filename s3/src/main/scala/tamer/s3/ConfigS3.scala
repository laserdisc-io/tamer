package tamer.s3

import cats.implicits._
import ciris.{ConfigException, ConfigValue, env}
import tamer.TamerError
import zio.interop.catz.{taskConcurrentInstance, zioContextShift}
import zio.{Has, Layer, Task}

object ConfigS3 {
  type S3Configuration = Has[S3Config]

  final case class S3Config(
      accessKeyId: Option[String],
      secretAccessKey: Option[String],
      profileName: Option[String],
      credentialProfilesFile: Option[String],
      region: Option[String],
      serviceEndpoint: Option[String]
  )

  private[this] val s3ConfigValue: ConfigValue[S3Config] = (
    env("ACCESS_KEY_ID").as[String].option,
    env("SECRET_ACCESS_KEY").as[String].option,
    env("PROFILE_NAME").as[String].option,
    env("CREDENTIAL_PROFILES_FILE").as[String].option,
    env("REGION").as[String].option,
    env("SERVICE_ENDPOINT").option
  ).parMapN(S3Config)

  val live: Layer[TamerError, S3Configuration] =
    s3ConfigValue.load[Task].refineToOrDie[ConfigException].mapError(ce => TamerError(ce.error.redacted.show, ce)).toLayer
}
