package tamer
package db

import java.sql.SQLException

import cats.effect.Blocker
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.util.query.Query0
import doobie.util.transactor.Transactor
import eu.timepit.refined.auto._
import tamer.config.DbConfig
import zio._
import zio.interop.catz._

import scala.concurrent.ExecutionContext

trait Db extends Serializable {
  val db: Db.Service[Any]
}

object Db {
  trait Service[R] {
    def runQuery[K, V](tnx: Transactor[Task], query: Query0[V], queue: Queue[(K, V)], f: V => K): ZIO[R, DbError, Unit]
  }

  trait Live extends Db {
    override final val db: Service[Any] = new Service[Any] {
      override final def runQuery[K, V](tnx: Transactor[Task], query: Query0[V], queue: Queue[(K, V)], f: V => K): IO[DbError, Unit] =
        query.stream.chunks.transact(tnx).evalTap(c => queue.offerAll(c.iterator.toStream.map(v => f(v) -> v))).compile.drain.mapError {
          case e: Exception => DbError(e.getLocalizedMessage)
        }
    }
  }

  def mkTransactor(db: DbConfig, connectEC: ExecutionContext, transactEC: ExecutionContext): Managed[DbError, HikariTransactor[Task]] =
    Managed {
      HikariTransactor
        .newHikariTransactor[Task](db.driver, db.uri, db.username, db.password, connectEC, Blocker.liftExecutionContext(transactEC))
        .allocated
        .map {
          case (ht, cleanup) => Reservation(ZIO.succeed(ht), _ => cleanup.orDie)
        }
        .uninterruptible
        .refineToOrDie[SQLException]
        .mapError(sqle => DbError(sqle.getLocalizedMessage()))
    }
}
