package tamer

import doobie.util.query.Query0
import doobie.util.transactor.Transactor
import zio.{Queue, Task, ZIO}

package object db extends Db.Service[Db] {
  override final def runQuery[K, V](tnx: Transactor[Task], query: Query0[V], queue: Queue[(K, V)], f: V => K): ZIO[Db, DbError, Unit] =
    ZIO.accessM(_.db.runQuery(tnx, query, queue, f))
}
