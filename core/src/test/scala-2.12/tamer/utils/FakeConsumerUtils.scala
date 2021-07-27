package tamer.utils

import org.apache.kafka.common.TopicPartition
import zio.Ref
import zio.stream.ZStream

object FakeConsumerUtils {
  def increaseForPartition(partition: TopicPartition, m: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    m.updated(partition, m(partition) + 1)
  }

  def getCommittedOr0(committed: Ref[Map[TopicPartition, Option[Long]]]): ZStream[Any, Nothing, Map[TopicPartition, Long]] = {
    ZStream.fromEffect(committed.get).map(_.mapValues(_.getOrElse(0L)).toMap)
  }
}
