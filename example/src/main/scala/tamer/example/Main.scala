package tamer
package example

import java.time.Instant
import java.time.temporal.ChronoUnit._

import doobie.implicits.javatime._
import doobie.syntax.string._
import zio.UIO

final case class State(from: Instant, to: Instant)
final case class Key(id: String)
final case class Value(id: String, name: String, description: Option[String], modifiedAt: Instant)

object Source {
  private[this] implicit final class InstantOps(private val instant: Instant) extends AnyVal {
    def plus5Minutes: Instant = instant.plus(5, MINUTES)
    def minus60Days: Instant  = instant.minus(60, DAYS)
    def orNow: UIO[Instant] =
      UIO(Instant.now).map {
        case now if instant.isAfter(now) => now
        case _                           => instant
      }
  }
  final val setup = UIO(Instant.now.truncatedTo(DAYS)).map { bootTime =>
    val sixtyDaysAgo = bootTime.minus60Days
    Setup.avro(
      State(sixtyDaysAgo, sixtyDaysAgo.plus5Minutes)
    )(s => sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${s.from} AND modified_at <= ${s.to}""".query[Value])(
      v => Key(v.id),
      s => {
        case QueryResult(_, Nil) => s.to.plus5Minutes.orNow.map(State(s.from, _))
        case QueryResult(_, values) =>
          val max = values.map(_.modifiedAt).max // if we can't order in the query we need to do it here...
          max.plus5Minutes.orNow.map(State(max, _))
      }
    )
  }
}

object Main extends TamerApp(Source.setup)
