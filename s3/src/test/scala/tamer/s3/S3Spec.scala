package tamer.s3

import zio.Ref
import zio.test.Assertion._
import zio.test.{DefaultRunnableSpec, ZSpec, _}

import java.time.format.DateTimeFormatterBuilder
import java.time.{ZoneId, ZonedDateTime}

object S3Spec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] = suite("S3Spec")(
    testM("Should be able to compute next state") {
      val afterwards        = LastProcessedInstant(ZonedDateTime.parse("2021-01-01T00:01:43+01:00[Europe/Rome]").toInstant)
      val dateTimeFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter().withZone(ZoneId.of("Europe/Rome"))
      val prefix            = "myFolder/myPrefix"
      val instant           = ZonedDateTime.parse("2021-01-01T00:01:44+01:00[Europe/Rome]").toInstant
      val makeKeys          = Ref.make(List("myFolder/myPrefix2021-01-01 00:01:44.empty"))

      makeKeys.flatMap(keysR => assertM(getNextInstant(keysR, afterwards, prefix, dateTimeFormatter))(isSome(equalTo(instant))))
    }
  )
}
