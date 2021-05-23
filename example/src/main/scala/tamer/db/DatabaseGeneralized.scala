package tamer
package db

import doobie.Query0
import doobie.implicits.legacy.instant._
import doobie.syntax.string._
import zio._
import zio.blocking.Blocking

import java.time.Instant
import scala.concurrent.duration._
import scala.util.hashing.byteswap64

final case class MyState(from: Instant, to: Instant)
object MyState {
  implicit final val codec                       = AvroCodec.codec[MyState]
  implicit final val hashable: Hashable[MyState] = s => (byteswap64(s.from.getEpochSecond) + byteswap64(s.to.getEpochSecond)).intValue
}

object DatabaseGeneralized extends App {
  implicit final val stringCodec = AvroCodec.codec[String]

  lazy val transactorLayer  = (Blocking.live ++ DbConfig.fromEnvironment) >>> db.hikariLayer
  lazy val queryConfigLayer = DbConfig.fromEnvironment
  lazy val kafkaConfig      = KafkaConfig.fromEnvironment
  lazy val myLayer          = transactorLayer ++ queryConfigLayer ++ kafkaConfig

  lazy val program = (for {
    bootTime <- UIO(Instant.now())
    setup = DbSetup(MyState(bootTime - 60.days, bootTime - 60.days + 5.minutes))(query)(
      keyExtract = _.id,
      stateFoldM = (s: MyState) => {
        case QueryResult(_, results) if results.isEmpty => (s.to + 5.minutes).orNow.map(MyState(s.from, _))
        case QueryResult(_, results) =>
          val mostRecent = results.sortBy(_.modifiedAt).max.timestamp
          (mostRecent + 5.minutes).orNow.map(MyState(mostRecent, _))
      }
    )
    _ <- DbTamer(setup).run
  } yield ()).mapError(e => TamerError("Could not run tamer example", e))

  private def query(s: MyState): Query0[Row] = {
    sql"""SELECT id, name, description, modified_at FROM users WHERE modified_at > ${s.from} AND modified_at <= ${s.to}""".query[Row]
  }

  override final def run(args: List[String]): URIO[ZEnv, ExitCode] = program.provideCustomLayer(myLayer).exitCode
}
