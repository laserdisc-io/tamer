package tamer
package s3

import java.time.Instant

final case class Line(str: String)
final case class LastProcessedInstant(instant: Instant)
