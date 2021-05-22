package tamer
package db

import doobie.syntax.string._
import zio._

import java.time.Instant
import scala.concurrent.duration._

object DatabaseSimple extends App {
  import doobie.implicits.legacy.instant._
  implicit final val stringCodec = AvroCodec.codec[String]

  val program: ZIO[ZEnv, TamerError, Unit] = (for {
    boot <- UIO(Instant.now())
    _ <- DbTamer.fetchWithTimeSegment(ts =>
      sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${ts.from} AND modified_at <= ${ts.to}""".query[Row]
    )(
      earliest = boot - 60.days,
      tumblingStep = 5.minutes,
      keyExtract = _.id
    )
  } yield ()).mapError(TamerError("Could not run tamer example", _))

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}
