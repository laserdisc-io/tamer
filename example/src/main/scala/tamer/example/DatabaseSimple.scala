package tamer
package example

import doobie.implicits.legacy.instant._
import doobie.syntax.string._
import tamer.db.Timestamped
import zio._

import java.time.temporal.ChronoUnit._
import java.time.{Duration, Instant}

final case class Key(id: String)
final case class Value(id: String, name: String, description: Option[String], modifiedAt: Instant) extends Timestamped(modifiedAt)

object DatabaseSimple extends zio.App {
  val program: ZIO[ZEnv, TamerError, Unit] = (for {
    boot <- UIO(Instant.now())
    _ <- tamer.db.fetchWithTimeSegment(ts =>
      sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${ts.from} AND modified_at <= ${ts.to}""".query[Value]
    )(
      earliest = boot.minus(60, DAYS),
      tumblingStep = Duration.of(5, MINUTES),
      keyExtract = (value: Value) => Key(value.id)
    )
  } yield ()).mapError(e => TamerError("Could not run tamer example", e))

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}
