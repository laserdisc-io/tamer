package tamer
package db

import java.time.Instant

import doobie.implicits.legacy.instant._
import doobie.syntax.string._
import zio._
import zio.duration._

object DatabaseSimple extends App {
  val program: ZIO[ZEnv, TamerError, Unit] = DbSetup
    .tumbling(window =>
      sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${window.from} AND modified_at <= ${window.to}""".query[Row]
    )(recordKey = (_, v) => v.id, from = Instant.parse("2020-01-01T00:00:00.00Z"), tumblingStep = 5.days)
    .runWith(dbLayerFromEnvironment ++ kafkaConfigFromEnvironment)

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}
