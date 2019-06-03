package org.apache.spark.streaming.dis

import com.huaweicloud.dis.adapter.kafka.common.TopicPartition
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

/**
  * Utilities for converting Kafka related objects to and from json.
  */
private object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  /**
    * Read TopicPartitions from json string
    */
  def partitions(str: String): Array[TopicPartition] = {
    try {
      Serialization.read[Map[String, Seq[Int]]](str).flatMap {  case (topic, parts) =>
        parts.map { part =>
          new TopicPartition(topic, part)
        }
      }.toArray
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"dis-stream1":[0],"dis-stream2":[0,1,2]}, got $str""")
    }
  }

  /**
    * Write TopicPartitions as json string
    */
  def partitions(partitions: Iterable[TopicPartition]): String = {
    val result = new HashMap[String, List[Int]]
    partitions.foreach { tp =>
      val parts: List[Int] = result.getOrElse(tp.topic, Nil)
      result += tp.topic -> (tp.partition::parts)
    }
    Serialization.write(result)
  }

  /**
    * Read per-TopicPartition offsets from json string
    */
  def partitionOffsets(str: String): Map[TopicPartition, Long] = {
    try {
      Serialization.read[Map[String, Map[Int, Long]]](str).flatMap { case (topic, partOffsets) =>
        partOffsets.map { case (part, offset) =>
          new TopicPartition(topic, part) -> offset
        }
      }.toMap
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"dis-stream1":{"0":23,"1":-1},"dis-stream2":{"0":-2}}, (-1 for latest, -2 for earliest), but got $str""")
    }
  }

  def partitionOffsetsByStreamName(streamName: String, partitions: String): Map[TopicPartition, Long] = {
    try {
      Serialization.read[Map[Int, Long]](partitions).map{ case (part, offset) =>
        new TopicPartition(streamName, part) -> offset
      }.toMap
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"0":23,"1":-1,"2":-2} (-1 for latest, -2 for earliest), but got $partitions""")
    }
  }

  def partitionOffsetRangesByStreamName(streamName: String, offsetRanges: String): Array[OffsetRange] = {
    try {
      Serialization.read[Map[Int, Array[Long]]](offsetRanges).map{ case (part, offsets) =>
        OffsetRange(streamName, part, offsets(0), offsets(1))
      }.toArray
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"0":[1,4],"1":[-2,-1]}, but got $offsetRanges. 
             |Format {"partitionNum1": [fromOffset1, untilOffset1], "partitionNum2": [fromOffset2, untilOffset2], ...} (-1 for latest, -2 for earliest),
           """.stripMargin)
    }
  }
  
  /**
    * Write per-TopicPartition offsets as json string
    */
  def partitionOffsets(partitionOffsets: Map[TopicPartition, Long]): String = {
    val result = new HashMap[String, HashMap[Int, Long]]()
    implicit val ordering = new Ordering[TopicPartition] {
      override def compare(x: TopicPartition, y: TopicPartition): Int = {
        Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
      }
    }
    val partitions = partitionOffsets.keySet.toSeq.sorted  // sort for more determinism
    partitions.foreach { tp =>
      val off = partitionOffsets(tp)
      val parts = result.getOrElse(tp.topic, new HashMap[Int, Long])
      parts += tp.partition -> off
      result += tp.topic -> parts
    }
    Serialization.write(result)
  }
}
