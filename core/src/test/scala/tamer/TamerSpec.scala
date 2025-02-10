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

import org.apache.kafka.common.TopicPartition
import zio._
import zio.kafka.admin.AdminClient.{TopicPartition => AdminPartition}
import zio.test._
import zio.test.Assertion.equalTo
import zio.test.TestAspect.{timeout, withLiveClock}

object TamerSpec extends ZIOSpecDefault with TamerSpecGen {

  case class Log(series: Vector[Int])
  object Log {
    val layer: ULayer[Ref[Log]] = ZLayer(Ref.make(Log(Vector.empty)))
  }

  val baseTamerLayer = Tamer.live {
    new Setup[Ref[Log], Key, Value, State] {
      override final val initialState = State(0)
      override final val stateKey     = 0
      override final def iteration(s: State, q: Enqueue[NonEmptyChunk[Record[Key, Value]]]): RIO[Ref[Log], State] =
        ZIO.service[Ref[Log]].flatMap { variable =>
          val cursor = s.state + 1
          if (cursor <= 10)
            variable.update(log => log.copy(log.series ++ Vector(cursor))) *>
              q.offer(NonEmptyChunk(Record(Key(cursor), Value(cursor)))).as(s.copy(state = cursor))
          else ZIO.never *> ZIO.succeed(State(9999))
        }
    }
  }

  val embeddedKafkaTamerLayer = FakeKafka.embeddedKafkaConfigLayer ++ Log.layer >>> baseTamerLayer

  val p = new TopicPartition("a-test-topic", 1)

  override final val spec = suite("TamerSpec")(
    test("should successfully run the iteration function 10 times") {
      for {
        outputVector <- ZIO.service[Ref[Log]]
        _            <- runLoop.timeout(7.seconds)
        result       <- outputVector.get
      } yield assert(result.series)(equalTo(Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
    } @@ timeout(20.seconds)
  ).provideSomeLayerShared[TestEnvironment](embeddedKafkaTamerLayer ++ Log.layer) @@ withLiveClock

}

private[tamer] sealed trait TamerSpecGen {

  val testPartition      = new TopicPartition("gen-test-topic", 1)
  val testAdminPartition = AdminPartition(testPartition)

  private[this] val committedOffset = Gen.long(0L, 100L).map(l => Map(testPartition -> l))

  val offsets: Gen[Sized, (Map[TopicPartition, Long], Map[TopicPartition, Long])] =
    committedOffset.flatMap { co =>
      Gen.long(0L, 100L).map(lag => co -> Map(testPartition -> (co(testPartition) + lag)))
    }
}
