/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.dis


import com.huaweicloud.dis.adapter.kafka.clients.consumer.OffsetCommitCallback
import com.huaweicloud.dis.adapter.kafka.common.TopicPartition
import org.apache.spark.annotation.Experimental

/**
 * Represents any object that has a collection of [[OffsetRange]]s. This can be used to access the
 * offset ranges in RDDs generated by the direct Kafka DStream (see
 * [[DISUtils.createDirectStream]]).
 * {{{
 *   KafkaUtils.createDirectStream(...).foreachRDD { rdd =>
 *      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
 *      ...
 *   }
 * }}}
 */
trait HasOffsetRanges {
  def offsetRanges: Array[OffsetRange]
}

/**
 *  :: Experimental ::
 * Represents any object that can commit a collection of [[OffsetRange]]s.
 * The direct Kafka DStream implements this interface (see
 * [[DISUtils.createDirectStream]]).
 * {{{
 *   val stream = KafkaUtils.createDirectStream(...)
 *     ...
 *   stream.asInstanceOf[CanCommitOffsets].commitAsync(offsets, new OffsetCommitCallback() {
 *     def onComplete(m: java.util.Map[TopicPartition, OffsetAndMetadata], e: Exception) {
 *        if (null != e) {
 *           // error
 *        } else {
 *         // success
 *       }
 *     }
 *   })
 * }}}
 */
@Experimental
trait CanCommitOffsets {
  /**
   *  :: Experimental ::
   * Queue up offset ranges for commit to Kafka at a future time.  Threadsafe.
   * This is only needed if you intend to store offsets in Kafka, instead of your own store.
   * @param offsetRanges The maximum untilOffset for a given partition will be used at commit.
   */
  @Experimental
  def commitAsync(offsetRanges: Array[OffsetRange]): Unit

  /**
   *  :: Experimental ::
   * Queue up offset ranges for commit to Kafka at a future time.  Threadsafe.
   * This is only needed if you intend to store offsets in Kafka, instead of your own store.
   * @param offsetRanges The maximum untilOffset for a given partition will be used at commit.
   * @param callback Only the most recently provided callback will be used at commit.
   */
  @Experimental
  def commitAsync(offsetRanges: Array[OffsetRange], callback: OffsetCommitCallback): Unit

  @Experimental
  def commitNow(offsetRanges: Array[OffsetRange]): Unit

  @Experimental
  def commitNow(offsetRanges: Array[OffsetRange], callback: OffsetCommitCallback): Unit

  @Experimental
  def commitNextDurationAsync(offsetRanges: Array[OffsetRange]): Unit

  @Experimental
  def commitNextDurationAsync(offsetRanges: Array[OffsetRange], callback: OffsetCommitCallback): Unit
}

/**
 * Represents a range of offsets from a single Kafka TopicPartition. Instances of this class
 * can be created with `OffsetRange.create()`.
 * @param topic Kafka topic name
 * @param partition Kafka partition id
 * @param fromOffset Inclusive starting offset
 * @param untilOffset Exclusive ending offset
 */
final class OffsetRange private(
    val topic: String,
    val partition: Int,
    val fromOffset: Long,
    val untilOffset: Long) extends Serializable {
  import OffsetRange.OffsetRangeTuple

  /** Kafka TopicPartition object, for convenience */
  def topicPartition(): TopicPartition = new TopicPartition(topic, partition)

  /** Number of messages this OffsetRange refers to */
  def count(): Long = untilOffset - fromOffset

  override def equals(obj: Any): Boolean = obj match {
    case that: OffsetRange =>
      this.topic == that.topic &&
        this.partition == that.partition &&
        this.fromOffset == that.fromOffset &&
        this.untilOffset == that.untilOffset
    case _ => false
  }

  override def hashCode(): Int = {
    toTuple.hashCode()
  }

  override def toString(): String = {
    s"OffsetRange(topic: '$topic', partition: $partition, range: [$fromOffset -> $untilOffset])"
  }

  /** this is to avoid ClassNotFoundException during checkpoint restore */
  private[streaming]
  def toTuple: OffsetRangeTuple = (topic, partition, fromOffset, untilOffset)
}

/**
 * Companion object the provides methods to create instances of [[OffsetRange]].
 */
object OffsetRange {
  def create(topic: String, partition: Int, fromOffset: Long, untilOffset: Long): OffsetRange =
    new OffsetRange(topic, partition, fromOffset, untilOffset)

  def create(
      topicPartition: TopicPartition,
      fromOffset: Long,
      untilOffset: Long): OffsetRange =
    new OffsetRange(topicPartition.topic, topicPartition.partition, fromOffset, untilOffset)

  def apply(topic: String, partition: Int, fromOffset: Long, untilOffset: Long): OffsetRange =
    new OffsetRange(topic, partition, fromOffset, untilOffset)

  def apply(
      topicPartition: TopicPartition,
      fromOffset: Long,
      untilOffset: Long): OffsetRange =
    new OffsetRange(topicPartition.topic, topicPartition.partition, fromOffset, untilOffset)

  /** this is to avoid ClassNotFoundException during checkpoint restore */
  private[dis]
  type OffsetRangeTuple = (String, Int, Long, Long)

  private[dis]
  def apply(t: OffsetRangeTuple) =
    new OffsetRange(t._1, t._2, t._3, t._4)
}
