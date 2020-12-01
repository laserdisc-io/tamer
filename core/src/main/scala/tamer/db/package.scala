package tamer

import doobie.util.transactor.Transactor
import zio.{Has, Queue, Task, ZIO}

package object db {
  type Db = Has[Db.Service]
  type DbTransactor = Has[Transactor[Task]]

  def runQuery[K, V, State](setup: Setup[K, V, State])(state: State, q: Queue[(K, V)]): ZIO[Db, TamerError, State] =
    ZIO.accessM(_.get.runQuery(setup)(state, q))
}
