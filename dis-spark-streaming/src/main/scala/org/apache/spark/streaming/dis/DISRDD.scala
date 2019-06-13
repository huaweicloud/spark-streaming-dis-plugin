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

import java.{util => ju}

import com.huaweicloud.dis.adapter.common.consumer.DisConsumerConfig
import com.huaweicloud.dis.adapter.kafka.clients.consumer.ConsumerRecord
import com.huaweicloud.dis.adapter.kafka.common.TopicPartition
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * A batch-oriented interface for consuming from Kafka.
 * Starting and ending offsets are specified in advance,
 * so that you can control exactly-once semantics.
 * @param kafkaParams Kafka
 * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
 * configuration parameters</a>. Requires "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
 * @param preferredHosts map from TopicPartition to preferred host for processing that partition.
 * In most cases, use [[LocationStrategies.PreferConsistent]]
 * Use [[LocationStrategies.PreferBrokers]] if your executors are on same nodes as brokers.
 * @param useConsumerCache whether to use a consumer from a per-jvm cache
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 */
private[spark] class DISRDD[K, V](
    sc: SparkContext,
    val kafkaParams: ju.Map[String, Object],
    val offsetRanges: Array[OffsetRange],
    val preferredHosts: ju.Map[TopicPartition, String],
    useConsumerCache: Boolean
) extends RDD[ConsumerRecord[K, V]](sc, Nil) with MyLogging with HasOffsetRanges {

  require("none" ==
    kafkaParams.get(DisConsumerConfig.AUTO_OFFSET_RESET_CONFIG).asInstanceOf[String],
    DisConsumerConfig.AUTO_OFFSET_RESET_CONFIG +
      " must be set to none for executor kafka params, else messages may not match offsetRange")

  require(false ==
    kafkaParams.get(DisConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).asInstanceOf[Boolean],
    DisConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG +
      " must be set to false for executor kafka params, else offsets may commit before processing")

  // TODO is it necessary to have separate configs for initial poll time vs ongoing poll time?
  private val pollTimeout = conf.getLong("spark.streaming.dis.consumer.poll.ms",
    conf.getTimeAsSeconds("spark.network.timeout", "120s") * 1000L)
  private val cacheInitialCapacity =
    conf.getInt("spark.streaming.dis.consumer.cache.initialCapacity", 16)
  private val cacheMaxCapacity =
    conf.getInt("spark.streaming.dis.consumer.cache.maxCapacity", 64)
  private val cacheLoadFactor =
    conf.getDouble("spark.streaming.dis.consumer.cache.loadFactor", 0.75).toFloat
  private val compacted =
    conf.getBoolean("spark.streaming.dis.allowNonConsecutiveOffsets", false)

  override def persist(newLevel: StorageLevel): this.type = {
    myLogError("Kafka ConsumerRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) =>
        new DISRDDPartition(i, o.topic, o.partition, o.fromOffset, o.untilOffset)
    }.toArray
  }

  override def count(): Long =
    if (compacted) {
      super.count()
    } else {
      offsetRanges.map(_.count).sum
    }

  override def countApprox(
      timeout: Long,
      confidence: Double = 0.95
  ): PartialResult[BoundedDouble] =
    if (compacted) {
      super.countApprox(timeout, confidence)
    } else {
      val c = count
      new PartialResult(new BoundedDouble(c, 1.0, c, c), true)
    }

  override def isEmpty(): Boolean =
    if (compacted) {
      super.isEmpty()
    } else {
      count == 0L
    }

  override def take(num: Int): Array[ConsumerRecord[K, V]] =
    if (compacted) {
      super.take(num)
    } else if (num < 1) {
      Array.empty[ConsumerRecord[K, V]]
    } else {
      val nonEmptyPartitions = this.partitions
        .map(_.asInstanceOf[DISRDDPartition])
        .filter(_.count > 0)

      if (nonEmptyPartitions.isEmpty) {
        Array.empty[ConsumerRecord[K, V]]
      } else {
        // Determine in advance how many messages need to be taken from each partition
        val parts = nonEmptyPartitions.foldLeft(Map[Int, Int]()) { (result, part) =>
          val remain = num - result.values.sum
          if (remain > 0) {
            val taken = Math.min(remain, part.count)
            result + (part.index -> taken.toInt)
          } else {
            result
          }
        }

        context.runJob(
          this,
          (tc: TaskContext, it: Iterator[ConsumerRecord[K, V]]) =>
          it.take(parts(tc.partitionId)).toArray, parts.keys.toArray
        ).flatten
      }
    }

  private def executors(): Array[ExecutorCacheTaskLocation] = {
    val bm = sparkContext.env.blockManager
    bm.master.getPeers(bm.blockManagerId).toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compareExecutors)
  }

  protected[dis] def compareExecutors(
      a: ExecutorCacheTaskLocation,
      b: ExecutorCacheTaskLocation): Boolean =
    if (a.host == b.host) {
      a.executorId > b.executorId
    } else {
      a.host > b.host
    }

  override def getPreferredLocations(thePart: Partition): Seq[String] = {
    // The intention is best-effort consistent executor for a given topicpartition,
    // so that caching consumers can be effective.
    // TODO what about hosts specified by ip vs name
    val part = thePart.asInstanceOf[DISRDDPartition]
    val allExecs = executors()
    val tp = part.topicPartition
    val prefHost = preferredHosts.get(tp)
    val prefExecs = if (null == prefHost) allExecs else allExecs.filter(_.host == prefHost)
    val execs = if (prefExecs.isEmpty) allExecs else prefExecs
    if (execs.isEmpty) {
      Seq.empty
    } else {
      // execs is sorted, tp.hashCode depends only on topic and partition, so consistent index
      val index = Math.floorMod(tp.hashCode, execs.length)
      val chosen = execs(index)
      Seq(chosen.toString)
    }
  }

  private def errBeginAfterEnd(part: DISRDDPartition): String =
    s"Beginning offset ${part.fromOffset} is after the ending offset ${part.untilOffset} " +
      s"for topic ${part.topic} partition ${part.partition}. " +
      "You either provided an invalid fromOffset, or the Kafka topic has been damaged"

  override def compute(thePart: Partition, context: TaskContext): Iterator[ConsumerRecord[K, V]] = {
    val part = thePart.asInstanceOf[DISRDDPartition]
    require(part.fromOffset <= part.untilOffset, errBeginAfterEnd(part))
    if (part.fromOffset == part.untilOffset) {
      myLogInfo(s"Beginning offset ${part.fromOffset} is the same as ending offset " +
        s"skipping ${part.topic} ${part.partition}")
      Iterator.empty
    } else {
      myLogInfo(s"Computing topic ${part.topic}, partition ${part.partition} " +
        s"offsets ${part.fromOffset} -> ${part.untilOffset}")
      if (compacted) {
        new CompactedDISRDDIterator[K, V](
          part,
          context,
          kafkaParams,
          useConsumerCache,
          pollTimeout,
          cacheInitialCapacity,
          cacheMaxCapacity,
          cacheLoadFactor
        )
      } else {
        new DISRDDIterator[K, V](
          part,
          context,
          kafkaParams,
          useConsumerCache,
          pollTimeout,
          cacheInitialCapacity,
          cacheMaxCapacity,
          cacheLoadFactor
        )
      }
    }
  }
}

/**
 * An iterator that fetches messages directly from Kafka for the offsets in partition.
 * Uses a cached consumer where possible to take advantage of prefetching
 */
private class DISRDDIterator[K, V](
  part: DISRDDPartition,
  context: TaskContext,
  kafkaParams: ju.Map[String, Object],
  useConsumerCache: Boolean,
  pollTimeout: Long,
  cacheInitialCapacity: Int,
  cacheMaxCapacity: Int,
  cacheLoadFactor: Float
) extends Iterator[ConsumerRecord[K, V]] {

  context.addTaskCompletionListener(_ => closeIfNeeded())

  val consumer = {
    DISDataConsumer.init(cacheInitialCapacity, cacheMaxCapacity, cacheLoadFactor)
    DISDataConsumer.acquire[K, V](part.topicPartition(), kafkaParams, context, useConsumerCache)
  }

  var requestOffset = part.fromOffset

  def closeIfNeeded(): Unit = {
    if (consumer != null) {
      consumer.release()
    }
  }

  override def hasNext(): Boolean = requestOffset < part.untilOffset

  override def next(): ConsumerRecord[K, V] = {
    if (!hasNext) {
      throw new ju.NoSuchElementException("Can't call getNext() once untilOffset has been reached")
    }
    val r = consumer.get(requestOffset, pollTimeout)
    requestOffset += 1
    r
  }
}

/**
 * An iterator that fetches messages directly from Kafka for the offsets in partition.
 * Uses a cached consumer where possible to take advantage of prefetching.
 * Intended for compacted topics, or other cases when non-consecutive offsets are ok.
 */
private class CompactedDISRDDIterator[K, V](
    part: DISRDDPartition,
    context: TaskContext,
    kafkaParams: ju.Map[String, Object],
    useConsumerCache: Boolean,
    pollTimeout: Long,
    cacheInitialCapacity: Int,
    cacheMaxCapacity: Int,
    cacheLoadFactor: Float
  ) extends DISRDDIterator[K, V](
    part,
    context,
    kafkaParams,
    useConsumerCache,
    pollTimeout,
    cacheInitialCapacity,
    cacheMaxCapacity,
    cacheLoadFactor
  ) {

  consumer.compactedStart(part.fromOffset, pollTimeout)

  private var nextRecord = consumer.compactedNext(pollTimeout)

  private var okNext: Boolean = true

  override def hasNext(): Boolean = okNext

  override def next(): ConsumerRecord[K, V] = {
    if (!hasNext) {
      throw new ju.NoSuchElementException("Can't call getNext() once untilOffset has been reached")
    }
    val r = nextRecord
    if (r.offset + 1 >= part.untilOffset) {
      okNext = false
    } else {
      nextRecord = consumer.compactedNext(pollTimeout)
      if (nextRecord.offset >= part.untilOffset) {
        okNext = false
        consumer.compactedPrevious()
      }
    }
    r
  }
}
