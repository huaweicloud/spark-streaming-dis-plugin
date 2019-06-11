package org.apache.spark.streaming.dis.example

import com.huaweicloud.dis.adapter.common.consumer.DisConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dis.{CanCommitOffsets, ConsumerStrategies, DISUtils, HasOffsetRanges}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DISSparkStreamingSubscribeExample {
  def main(args: Array[String]): Unit = {
    println("Start DIS Spark Streaming Demo.")
    if (args.length < 9) {
      println(s"args is wrong, should be [endpoint region ak sk projectId streamName startingOffsets duration groupId]".stripMargin)
      return
    }

    // DIS Endpoint
    // Region ID
    // Access Key Id
    // Secret Access Key
    // User ProjectId
    // DIS stream name
    // Starting offsets:  'LATEST'    (Starting with the latest sequenceNumber) 
    //                    'EARLIEST'  (Starting with the earliest sequenceNumber)
    // Duration: StreamingContext duration(second)
    // GroupId: DIS App_name
    val (endpoint, region, ak, sk, projectId, streamName, startingOffsets, duration, groupId)
    = (args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8))

    val sparkConf = new SparkConf().setAppName("Spark streaming DIS Subscribe example")
    val ssc = new StreamingContext(sparkConf, Seconds(duration.toInt))

    val params = Map(
      DISUtils.PROPERTY_ENDPOINT -> endpoint,
      DISUtils.PROPERTY_REGION_ID -> region,
      DISUtils.PROPERTY_AK -> ak,
      DISUtils.PROPERTY_SK -> sk,
      DISUtils.PROPERTY_PROJECT_ID -> projectId,
      DisConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> startingOffsets,
      DisConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      DisConsumerConfig.GROUP_ID_CONFIG -> groupId)

    val stream = DISUtils.createDirectStream[String, String](
      ssc, ConsumerStrategies.Subscribe[String, String](Array(streamName), params))

    // word count
    stream.map(_.value).flatMap(_.split(" ")).map(x => (x, 1L)).reduceByKey(_ + _).print()

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // commit offset to DIS async.
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    //start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}