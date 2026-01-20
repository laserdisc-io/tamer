/*
 * Copyright (c) 2019-2026 LaserDisc
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

import org.apache.kafka.common.TopicPartition
import zio._
import zio.test._
import zio.test.Assertion.equalTo
import zio.test.TestAspect.{timeout, withLiveClock}

object TamerSpec extends ZIOSpecDefault {

  case class Log(series: Vector[Int])
  object Log {
    val layer: ULayer[Ref[Log]] = ZLayer(Ref.make(Log(Vector.empty)))
  }

  implicit val intCodec: Codec[Int] = Codec.optionalVulcanCodec

  val baseTamerLayer = Tamer.live {
    new Setup[Ref[Log], Int, Int, Int] {
      override final val initialState                                                                               = 0
      override final val stateKey                                                                                   = 0
      override final def iteration(state: Int, queue: Enqueue[NonEmptyChunk[Record[Int, Int]]]): RIO[Ref[Log], Int] =
        ZIO.service[Ref[Log]].flatMap { logRef =>
          val cursor = state + 1
          if (cursor <= 10)
            logRef.update(log => log.copy(log.series :+ cursor)) *>
              queue.offer(NonEmptyChunk(Record(cursor, cursor))).as(cursor)
          else ZIO.never
        }
    }
  }

  val embeddedKafkaTamerLayer = FakeKafka.embeddedKafkaConfigLayer ++ Log.layer >>> baseTamerLayer

  val p = new TopicPartition("a-test-topic", 1)

  override final val spec = suite("TamerSpec")(
    test("should successfully run the iteration function 10 times") {
      for {
        outputVector <- ZIO.service[Ref[Log]]
        _            <- runLoop.timeout(7.seconds).fork
        _            <- ZIO.sleep(5.seconds)
        result       <- outputVector.get
      } yield assert(result.series)(equalTo(Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
    }
  ).provideSomeLayer[TestEnvironment](embeddedKafkaTamerLayer ++ Log.layer) @@ timeout(20.seconds) @@ withLiveClock
}
