package tamer
package db

import java.time.Instant

import doobie.implicits.legacy.instant._
import doobie.syntax.string._
import zio._
import zio.duration._

final case class MyState(from: Instant, to: Instant)
object MyState {
  implicit final val hashable: Hashable[MyState] = s => s.from.hash + s.to.hash
}

object DatabaseGeneralized extends App {
  val program: ZIO[ZEnv, TamerError, Unit] = for {
    bootTime <- UIO(Instant.now())
    tamer <- DbSetup(MyState(bootTime - 60.days, bootTime - 60.days + 5.minutes))(s =>
      sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${s.from} AND modified_at <= ${s.to}""".query[Row]
    )(
      recordKey = (_, v) => v.id,
      stateFold = {
        case (s, QueryResult(_, results)) if results.isEmpty => UIO(MyState(s.from, (s.to + 5.minutes).orNow()))
        case (_, QueryResult(_, results)) =>
          val mostRecent = results.sortBy(_.modifiedAt).max.timestamp
          UIO(MyState(mostRecent, (mostRecent + 5.minutes).orNow()))
      }
    ).runWith(dbLayerFromEnvironment ++ kafkaConfigFromEnvironment)
  } yield tamer

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.exitCode
}
