package tamer

import doobie.util.transactor.Transactor
import zio.{Queue, Task, ZIO}

package object db extends Db.Service[Db] {
  override final def runQuery[K, V, State](
      tnx: Transactor[Task],
      setup: Setup[K, V, State]
  )(
      state: State,
      q: Queue[(K, V)]
  ): ZIO[Db, DbError, State] = ZIO.accessM(_.db.runQuery(tnx, setup)(state, q))
}
