/*
 * Copyright (c) 2019-2025 LaserDisc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

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
