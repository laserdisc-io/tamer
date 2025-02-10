package tamer
package db

import doobie.generic.auto._
import doobie.implicits.legacy.instant._
import doobie.syntax.string._
import zio._

object DatabaseGeneralized extends ZIOAppDefault {
  import implicits._

  override final val run = Clock.instant.flatMap { bootTime =>
    DbSetup(MyState(bootTime - 60.days, bootTime - 60.days + 5.minutes))(s =>
      sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${s.from} AND modified_at <= ${s.to}""".query[Row]
    )(
      recordFrom = (_, v) => Record(v.id, v),
      stateFold = {
        case (s, QueryResult(_, results)) if results.isEmpty => Clock.instant.map(now => MyState(s.from, (s.to + 5.minutes).or(now)))
        case (_, QueryResult(_, results)) =>
          val mostRecent = results.sortBy(_.modifiedAt).max.timestamp
          Clock.instant.map(now => MyState(mostRecent, (mostRecent + 5.minutes).or(now)))
      }
    ).runWith(dbLayerFromEnvironment ++ KafkaConfig.fromEnvironment)
  }
}
