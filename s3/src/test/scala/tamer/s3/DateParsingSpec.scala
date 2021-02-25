package tamer.s3

import tamer.s3.S3Configuration.{parseInstantFromKey, suffixWithoutFileExtension}
import zio.test.Assertion._
import zio.test._

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, FormatStyle}
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Locale

object DateParsingSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] = suite("DateParsingSpec")(
    test("Should compute suffix") {
      val date              = LocalDateTime.parse("2021-01-01T00:01:44")
      val dateTimeFormatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).localizedBy(Locale.ITALY).withZone(ZoneId.of("Europe/Rome"))
      val prefix            = "myFolder/myPrefix"
      val key               = s"myFolder/myPrefix${date.format(dateTimeFormatter)}.empty"

      assert(suffixWithoutFileExtension(key, prefix, dateTimeFormatter))(equalTo("01/01/21, 00:01"))
    },
    test("Should compute suffix even if complex") {
      val date              = LocalDateTime.parse("2021-01-01T00:01:44")
      val dateTimeFormatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).localizedBy(Locale.GERMANY).withZone(ZoneId.of("Europe/Rome"))
      val prefix            = "myFolder/myPrefix"
      val key               = s"myFolder/myPrefix${date.format(dateTimeFormatter)}.empty.tar.gz"

      assert(suffixWithoutFileExtension(key, prefix, dateTimeFormatter))(equalTo("01.01.21, 00:01"))
    },
    test("Should parse date in simple case") {
      val prefix            = "myFolder/myPrefix"
      val key               = "myFolder/myPrefix2021-01-01 00:01:44.empty"
      val dateTimeFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter().withZone(ZoneId.of("Europe/Rome"))

      assert(parseInstantFromKey(key, prefix, dateTimeFormatter))(equalTo(ZonedDateTime.parse("2021-01-01T00:01:44+01:00[Europe/Rome]").toInstant))
    }
  )
}
