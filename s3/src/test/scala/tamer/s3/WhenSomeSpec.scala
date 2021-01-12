package tamer
package s3

import zio.console.putStr
import zio.test.Assertion.equalTo
import zio.test._
import zio.test.environment.TestConsole

object WhenSomeSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] = suite("whenSomeSpec")(
    testM("should fire when Some") {
      val program = whenSome(Some("a"))(a => putStr(s"hi $a"))
      assertM(program *> TestConsole.output)(equalTo(Vector("hi a")))
    }
  )
}
