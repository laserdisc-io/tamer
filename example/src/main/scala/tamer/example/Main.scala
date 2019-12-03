package tamer
package example

import doobie.syntax.string._
import zio.UIO

final case class User(id: Int, name: String)

object Main extends TamerApp(UIO(Setup.avroSimpleK(User(0, "god"))(_ => sql"SELECT id, name FROM users".query[User], _.id)))
