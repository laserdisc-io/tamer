package tamer.s3

import zio.Ref
import zio.test.Assertion._
import zio.test._

import java.time.format.DateTimeFormatterBuilder
import java.time.{ZoneId, ZonedDateTime}

object DateParsingSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] = suite("DateParsingSpec")(
    test("Should compute suffix") {
      val prefix = "myFolder/myPrefix"
      val key    = "myFolder/myPrefix2021-01-01 00:01:44.empty"

      assert(S3.suffixWithoutFileExtension(key, prefix))(equalTo("2021-01-01 00:01:44"))
    },
    test("Should parse date in simple case") {
      val prefix            = "myFolder/myPrefix"
      val key               = "myFolder/myPrefix2021-01-01 00:01:44.empty"
      val dateTimeFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter().withZone(ZoneId.of("Europe/Rome"))

      assert(S3.parseInstantFromKey(key, prefix, dateTimeFormatter))(equalTo(ZonedDateTime.parse("2021-01-01T00:01:44+01:00[Europe/Rome]").toInstant))
    },
    test("Should derive key in simple case") {
      val instant           = ZonedDateTime.parse("2021-01-01T00:01:44+01:00[Europe/Rome]").toInstant
      val dateTimeFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter().withZone(ZoneId.of("Europe/Rome"))
      val keys              = List("myFolder/myPrefix2021-01-01 00:01:44.empty")

      assert(S3.deriveKey(instant, dateTimeFormatter, keys))(isSome(equalTo("myFolder/myPrefix2021-01-01 00:01:44.empty")))
    },
    testM("Should be able to compute next state") { // TODO: move in other suite
      val afterwards        = LastProcessedInstant(ZonedDateTime.parse("2021-01-01T00:01:43+01:00[Europe/Rome]").toInstant)
      val dateTimeFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter().withZone(ZoneId.of("Europe/Rome"))
      val prefix            = "myFolder/myPrefix"
      val instant           = ZonedDateTime.parse("2021-01-01T00:01:44+01:00[Europe/Rome]").toInstant
      val makeKeys          = Ref.make(List("myFolder/myPrefix2021-01-01 00:01:44.empty"))

      makeKeys.flatMap(keysR => assertM(S3.getNextInstant(keysR, afterwards, prefix, dateTimeFormatter))(isSome(equalTo(instant))))
    }
  )
}
