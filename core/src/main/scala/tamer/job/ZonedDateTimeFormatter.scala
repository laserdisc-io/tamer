package tamer.job

import java.time.ZoneId
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

case class ZonedDateTimeFormatter private (value: DateTimeFormatter)

object ZonedDateTimeFormatter {
  private def apply(value: DateTimeFormatter): ZonedDateTimeFormatter = new ZonedDateTimeFormatter(value)

  def apply(dateTimeFormatter: DateTimeFormatter, zoneId: ZoneId): ZonedDateTimeFormatter =
    apply(dateTimeFormatter.withZone(zoneId))

  def fromPattern(pattern: String, zoneId: ZoneId): ZonedDateTimeFormatter =
    apply(new DateTimeFormatterBuilder().appendPattern(pattern).toFormatter, zoneId)
}
