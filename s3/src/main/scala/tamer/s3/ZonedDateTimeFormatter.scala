package tamer.s3

import java.time.ZoneId
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.TemporalAccessor

final class ZonedDateTimeFormatter private (private val _underlying: DateTimeFormatter) extends AnyVal {
  final def format(temporalAccessor: TemporalAccessor): String = _underlying.format(temporalAccessor)
  final def parse(s: String): TemporalAccessor                 = _underlying.parse(s)
}

object ZonedDateTimeFormatter {
  def apply(dateTimeFormatter: DateTimeFormatter, zoneId: ZoneId): ZonedDateTimeFormatter =
    new ZonedDateTimeFormatter(dateTimeFormatter.withZone(zoneId))

  def fromPattern(pattern: String, zoneId: ZoneId): ZonedDateTimeFormatter =
    apply(new DateTimeFormatterBuilder().appendPattern(pattern).toFormatter, zoneId)
}
