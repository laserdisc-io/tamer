package utils

import org.apache.kafka.common.TopicPartition
import zio.Ref
import zio.stream.ZStream

object FakeConsumerUtils {
  def increaseForPartition[V, K, R](partition: TopicPartition, m: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    m.updatedWith(partition)(_.map(_ + 1))
  }

  def getCommittedOr0[V, K, R](committed: Ref[Map[TopicPartition, Option[Long]]]): ZStream[Any, Nothing, Map[TopicPartition, Long]] = {
    ZStream.fromEffect(committed.get).map(_.view.mapValues(_.getOrElse(0L)).toMap)
  }
}
