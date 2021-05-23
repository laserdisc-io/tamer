package tamer
package db

import doobie.implicits.legacy.instant._
import doobie.syntax.string._
import zio._

import java.time.Instant
import scala.concurrent.duration._

object DatabaseSimple extends App {
  implicit final val stringCodec = AvroCodec.codec[String]

  val program: ZIO[ZEnv, TamerError, Unit] = DbTamer.fetchWithTimeSegment(ts =>
      sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${ts.from} AND modified_at <= ${ts.to}""".query[Row]
    )(
      earliest = Instant.parse("2020-01-01T00:00:00.00Z"),
      tumblingStep = 5.days,
      keyExtract = _.id
    ).mapError(TamerError("Could not run tamer example", _))

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}
