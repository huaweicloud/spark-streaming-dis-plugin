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

import com.huaweicloud.dis.adapter.kafka.consumer.{DISKafkaConsumer, Fetcher}
import com.huaweicloud.dis.iface.stream.request.DescribeStreamRequest
import com.huaweicloud.dis.{DISClient, DISConfig}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream._

/**
 * :: Experimental ::
 * object for constructing Kafka streams and RDDs
 */
@Experimental
object DISUtils extends Logging {
  /**
   * :: Experimental ::
   * Scala constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createRDD[K, V](
      sc: SparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy
    ): RDD[ConsumerRecord[K, V]] = {
    val preferredHosts = locationStrategy match {
      case PreferBrokers =>
        throw new AssertionError(
          "If you want to prefer brokers, you must provide a mapping using PreferFixed " +
          "A single DISRDD does not have a driver consumer and cannot look up brokers for you.")
      case PreferConsistent => ju.Collections.emptyMap[TopicPartition, String]()
      case PreferFixed(hostMap) =>         
        throw new AssertionError(
        "DIS dose not support PreferFixed(hostMap), please use PreferConsistent.")
    }
    val kp = new ju.HashMap[String, Object](kafkaParams)
    fixKafkaParams(kp)
    val osr = offsetRanges.clone()
    logInfo(s"OffsetRanges is ${offsetRanges.deep.mkString(", ")}")
    new DISRDD[K, V](sc, kp, osr, preferredHosts, true)
  }
  
  /**
   * :: Experimental ::
   * Java constructor for a batch-oriented interface for consuming from Kafka.
   * Starting and ending offsets are specified in advance,
   * so that you can control exactly-once semantics.
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a>. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createRDD[K, V](
      jsc: JavaSparkContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      locationStrategy: LocationStrategy
    ): JavaRDD[ConsumerRecord[K, V]] = {

    new JavaRDD(createRDD[K, V](jsc.sc, kafkaParams, offsetRanges, locationStrategy))
  }

  /**
   * :: Experimental ::
   * Scala constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
   *  of messages
   * per second that each '''partition''' will accept.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createDirectStream[K, V](
      ssc: StreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V]
    ): InputDStream[ConsumerRecord[K, V]] = {
    val ppc = new DefaultPerPartitionConfig(ssc.sparkContext.getConf)
    createDirectStream[K, V](ssc, locationStrategy, consumerStrategy, ppc)
  }

  @Experimental
  def createDirectStream[K, V](
                                ssc: StreamingContext,
                                consumerStrategy: ConsumerStrategy[K, V]
                              ): InputDStream[ConsumerRecord[K, V]] = {
    val ppc = new DefaultPerPartitionConfig(ssc.sparkContext.getConf)
    createDirectStream[K, V](ssc, LocationStrategies.PreferConsistent, consumerStrategy, ppc)
  }
  
  /**
   * :: Experimental ::
   * Scala constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details.
   * @param perPartitionConfig configuration of settings such as max rate on a per-partition basis.
   *   see [[PerPartitionConfig]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createDirectStream[K, V](
      ssc: StreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V],
      perPartitionConfig: PerPartitionConfig
    ): InputDStream[ConsumerRecord[K, V]] = {
    new DirectDISInputDStream[K, V](ssc, locationStrategy, consumerStrategy, perPartitionConfig)
  }

  @Experimental
  def createDirectStream[K, V](
                                jssc: JavaStreamingContext,
                                consumerStrategy: ConsumerStrategy[K, V]
                              ): JavaInputDStream[ConsumerRecord[K, V]] = {
    new JavaInputDStream(
      createDirectStream[K, V](
        jssc.ssc, PreferConsistent, consumerStrategy))
  }
  
  /**
   * :: Experimental ::
   * Java constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createDirectStream[K, V](
      jssc: JavaStreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V]
    ): JavaInputDStream[ConsumerRecord[K, V]] = {
    new JavaInputDStream(
      createDirectStream[K, V](
        jssc.ssc, locationStrategy, consumerStrategy))
  }

  /**
   * :: Experimental ::
   * Java constructor for a DStream where
   * each given Kafka topic/partition corresponds to an RDD partition.
   * @param locationStrategy In most cases, pass in [[LocationStrategies.PreferConsistent]],
   *   see [[LocationStrategies]] for more details.
   * @param consumerStrategy In most cases, pass in [[ConsumerStrategies.Subscribe]],
   *   see [[ConsumerStrategies]] for more details
   * @param perPartitionConfig configuration of settings such as max rate on a per-partition basis.
   *   see [[PerPartitionConfig]] for more details.
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   */
  @Experimental
  def createDirectStream[K, V](
      jssc: JavaStreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V],
      perPartitionConfig: PerPartitionConfig
    ): JavaInputDStream[ConsumerRecord[K, V]] = {
    new JavaInputDStream(
      createDirectStream[K, V](
        jssc.ssc, locationStrategy, consumerStrategy, perPartitionConfig))
  }

  /**
   * Tweak kafka params to prevent issues on executors
   */
  private[dis] def fixKafkaParams(kafkaParams: ju.HashMap[String, Object]): Unit = {
    logWarning(s"overriding ${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG} to false for executor")
    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)

    logWarning(s"overriding ${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG} to none for executor")
    kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")

    // driver and executor should be in different consumer groups
    val originalGroupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG)
    if (null == originalGroupId) {
      logError(s"${ConsumerConfig.GROUP_ID_CONFIG} is null, you should probably set it")
    }
    val groupId = "spark-executor-" + originalGroupId
    logWarning(s"overriding executor ${ConsumerConfig.GROUP_ID_CONFIG} to ${groupId}")
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    // possible workaround for KAFKA-3135
    val rbb = kafkaParams.get(ConsumerConfig.RECEIVE_BUFFER_CONFIG)
    if (null == rbb || rbb.asInstanceOf[java.lang.Integer] < 65536) {
      logWarning(s"overriding ${ConsumerConfig.RECEIVE_BUFFER_CONFIG} to 65536 see KAFKA-3135")
      kafkaParams.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
    }
  }

  @Experimental
  def createRDD[K, V](
                       sc: SparkContext,
                       disParams: ju.Map[String, Object],
                       streamName: String,
                       offsetRanges: String,
                       locationStrategy: LocationStrategy
                     ): RDD[ConsumerRecord[K, V]] = {

    val offsets = JsonUtils.partitionOffsetRangesByStreamName(streamName, offsetRanges)

    val topicPartitions = new ju.ArrayList[TopicPartition]()
    val consumer = new DISKafkaConsumer[K, V](getDisMap(disParams))

    val osr = offsets.map(r => {
      val topicPartition = new TopicPartition(r.topic, r.partition)
      topicPartitions.clear()
      topicPartitions.add(topicPartition)
      consumer.assign(topicPartitions)
      var fromOffset = r.fromOffset
      var untilOffset = r.untilOffset

      if (fromOffset == -1 || untilOffset == -1) {
        consumer.seekToEnd(topicPartitions)
        val endOffset = consumer.position(topicPartition)
        if (fromOffset == -1) {
          fromOffset = endOffset
          log.info(s"Stream ${r.topic} partition ${r.partition} fromOffset set to -1, overriding fromOffset to $fromOffset")
        }
        if (untilOffset == -1) {
          untilOffset = endOffset
          log.info(s"Stream ${r.topic} partition ${r.partition} untilOffset set to -1, overriding untilOffset to $untilOffset")
        }
      }

      if (fromOffset == -2 || untilOffset == -2) {
        consumer.seekToBeginning(topicPartitions)
        val startOffset = consumer.position(topicPartition)
        if (fromOffset == -2) {
          fromOffset = startOffset
          log.info(s"Stream ${r.topic} partition ${r.partition} fromOffset set to -2, overriding fromOffset to $fromOffset")
        }

        if (untilOffset == -2) {
          untilOffset = startOffset
          log.info(s"Stream ${r.topic} partition ${r.partition} untilOffset set to -2, overriding untilOffset to $untilOffset")
        }
      }

      OffsetRange(r.topic, r.partition, fromOffset, untilOffset)
    })
    consumer.close()

    createRDD(sc, disParams, osr, locationStrategy)
  }

  @Experimental
  def createRDD[K, V](
                       sc: SparkContext,
                       disParams: ju.Map[String, Object],
                       streamName: String,
                       offsetRanges: String
                     ): RDD[ConsumerRecord[K, V]] = {
    createRDD(sc, disParams, streamName, offsetRanges, LocationStrategies.PreferConsistent)
  }
  
  def getDisMap(disParams: ju.Map[String, Object]): ju.HashMap[String, Object] = {
      val disConfig = new ju.HashMap[String, Object]
      disConfig.putAll(disParams)
      updateDisConfigParam(disConfig, DISConfig.PROPERTY_REGION_ID, disParams.get(PROPERTY_REGION_ID), true)
      updateDisConfigParam(disConfig, DISConfig.PROPERTY_AK, disParams.get(PROPERTY_AK), true)
      updateDisConfigParam(disConfig, DISConfig.PROPERTY_SK, disParams.get(PROPERTY_SK), true)
      updateDisConfigParam(disConfig, DISConfig.PROPERTY_PROJECT_ID, disParams.get(PROPERTY_PROJECT_ID), true)
      updateDisConfigParam(disConfig, DISConfig.PROPERTY_ENDPOINT, disParams.get(PROPERTY_ENDPOINT), false)
      updateDisConfigParam(disConfig, DISConfig.PROPERTY_IS_DEFAULT_TRUSTED_JKS_ENABLED,
        disParams.getOrDefault(PROPERTY_IS_DEFAULT_TRUSTED_JKS_ENABLED, "false"), false)
      updateDisConfigParam(disConfig, Fetcher.KEY_MAX_PARTITION_FETCH_RECORDS,
        disParams.getOrDefault(PROPERTY_MAX_PARTITION_FETCH_RECORDS, "500"), false)
      updateDisConfigParam(disConfig, Fetcher.KEY_MAX_FETCH_THREADS,
        disParams.getOrDefault(PROPERTY_MAX_FETCH_THREADS, "1"), false)
      updateDisConfigParam(disConfig, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        disParams.getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "LATEST").toString.toUpperCase, false)
      updateDisConfigParam(disConfig, DISConfig.PROPERTY_CONNECTION_TIMEOUT,
        disParams.get(PROPERTY_CONNECTION_TIMEOUT), false)
      updateDisConfigParam(disConfig, DISConfig.PROPERTY_SOCKET_TIMEOUT,
        disParams.get(PROPERTY_SOCKET_TIMEOUT), false)
      updateDisConfigParam(disConfig, DISConfig.PROPERTY_BODY_SERIALIZE_TYPE,
        disParams.get(PROPERTY_BODY_SERIALIZE_TYPE), false)
      updateDisConfigParam(disConfig, DISConfig.PROPERTY_IS_DEFAULT_DATA_ENCRYPT_ENABLED,
        disParams.get(PROPERTY_IS_DEFAULT_DATA_ENCRYPT_ENABLED), false)
      updateDisConfigParam(disConfig, DISConfig.PROPERTY_DATA_PASSWORD,
        disParams.get(PROPERTY_DATA_PASSWORD), false)
      updateDisConfigParam(disConfig, DISConfig.PROPERTY_CONFIG_PROVIDER_CLASS,
        disParams.get(PROPERTY_CONFIG_PROVIDER_CLASS), false)
      updateDisConfigParam(disConfig, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        disParams.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"), false)
      disConfig
  }

  private def updateDisConfigParam(disConfig: ju.Map[String, Object], param: String, value: Any, isRequired: Boolean): Any = {
    if (value == null) {
      if (isRequired) throw new IllegalArgumentException("param " + param + " is null.")
      return
    }
    disConfig.put(param, value.toString)
  }

  def getTopicPartitions(streamName: String, disParams: ju.Map[String, Object]): Iterable[TopicPartition] = {
    val config = new DISConfig()
    config.putAll(getDisMap(disParams))
    val disClient = new DISClient(config)
    var topicPartitions = List[TopicPartition]()

    val describeStreamRequest = new DescribeStreamRequest
    describeStreamRequest.setStreamName(streamName)
    describeStreamRequest.setLimitPartitions(1)
    val describeStreamResult = disClient.describeStream(describeStreamRequest)
    for (index <- 0 until describeStreamResult.getReadablePartitionCount) {
      topicPartitions = topicPartitions :+ new TopicPartition(streamName, index)
    }

    logInfo(s"DIS Stream [$streamName] has ${topicPartitions.size} partitions.")
    topicPartitions
  }

  val PROPERTY_REGION_ID = DISConfig.PROPERTY_REGION_ID
  val PROPERTY_ENDPOINT = DISConfig.PROPERTY_ENDPOINT
  val PROPERTY_PROJECT_ID = DISConfig.PROPERTY_PROJECT_ID
  val PROPERTY_AK = DISConfig.PROPERTY_AK
  val PROPERTY_SK = DISConfig.PROPERTY_SK
  val PROPERTY_CONNECTION_TIMEOUT = DISConfig.PROPERTY_CONNECTION_TIMEOUT
  val PROPERTY_SOCKET_TIMEOUT = DISConfig.PROPERTY_SOCKET_TIMEOUT
  val PROPERTY_MAX_PER_ROUTE = DISConfig.PROPERTY_MAX_PER_ROUTE
  val PROPERTY_MAX_TOTAL = DISConfig.PROPERTY_MAX_TOTAL
  val PROPERTY_IS_DEFAULT_TRUSTED_JKS_ENABLED = DISConfig.PROPERTY_IS_DEFAULT_TRUSTED_JKS_ENABLED
  val PROPERTY_IS_DEFAULT_DATA_ENCRYPT_ENABLED = DISConfig.PROPERTY_IS_DEFAULT_DATA_ENCRYPT_ENABLED
  val PROPERTY_BODY_SERIALIZE_TYPE = DISConfig.PROPERTY_BODY_SERIALIZE_TYPE
  val PROPERTY_CONFIG_PROVIDER_CLASS = DISConfig.PROPERTY_CONFIG_PROVIDER_CLASS
  val PROPERTY_DATA_PASSWORD = DISConfig.PROPERTY_DATA_PASSWORD
  val PROPERTY_AUTO_OFFSET_RESET = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
  val PROPERTY_MAX_PARTITION_FETCH_RECORDS = Fetcher.KEY_MAX_PARTITION_FETCH_RECORDS
  val PROPERTY_MAX_FETCH_THREADS = Fetcher.KEY_MAX_FETCH_THREADS
  val PROPERTY_KEY_DESERIALIZER_CLASS = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
  val PROPERTY_VALUE_DESERIALIZER_CLASS = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
}
