package tamer
package s3

import zio.{Queue, Ref}
import zio.test._
import zio.test.Assertion._

import java.time.format.DateTimeFormatterBuilder
import java.time.{ZoneId, ZonedDateTime}

object S3Spec extends DefaultRunnableSpec {
  override final val spec = suite("S3Spec")(
    testM("Should be able to compute next state") {
      val afterwards        = LastProcessedInstant(ZonedDateTime.parse("2021-01-01T00:01:43+01:00[Europe/Rome]").toInstant)
      val dateTimeFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter().withZone(ZoneId.of("Europe/Rome"))
      val prefix            = "myFolder/myPrefix"
      val instant           = ZonedDateTime.parse("2021-01-01T00:01:44+01:00[Europe/Rome]").toInstant
      val makeKeys          = Ref.make(List("myFolder/myPrefix2021-01-01 00:01:44.empty"))
      val makeQueue         = Queue.dropping[Unit](requestedCapacity = 1).tap(_.offer(()))

      (makeQueue <*> makeKeys).flatMap { case (keysQ, keysR) =>
        assertM(S3Setup.getNextState(prefix, dateTimeFormatter)(keysR, afterwards, keysQ).map(_.instant))(equalTo(instant))
      }
    }
  )
}
