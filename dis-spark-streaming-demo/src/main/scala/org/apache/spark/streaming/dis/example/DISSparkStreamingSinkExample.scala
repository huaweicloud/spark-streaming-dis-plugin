package org.apache.spark.streaming.dis.example

import java.util.concurrent.Future

import com.huaweicloud.dis.adapter.common.consumer.DisConsumerConfig
import com.huaweicloud.dis.adapter.kafka.clients.producer.RecordMetadata
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dis._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DISSparkStreamingSinkExample {
  def main(args: Array[String]): Unit = {
    println("Start DIS Spark Streaming Sink Demo.")
    if (args.length < 8) {
      println(s"args is wrong, should be [endpoint region ak sk projectId streamName startingOffsets duration]".stripMargin)
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
    val (endpoint, region, ak, sk, projectId, streamName, startingOffsets, duration)
    = (args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))

    val sparkConf = new SparkConf().setAppName("Spark streaming DIS Sink example")
    val ssc = new StreamingContext(sparkConf, Seconds(duration.toInt))

    val params = Map(
      DISUtils.PROPERTY_ENDPOINT -> endpoint,
      DISUtils.PROPERTY_REGION_ID -> region,
      DISUtils.PROPERTY_AK -> ak,
      DISUtils.PROPERTY_SK -> sk,
      DISUtils.PROPERTY_PROJECT_ID -> projectId,
      DisConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> startingOffsets,
      DisConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

    val disProducer: Broadcast[DISSink[Array[Byte], String]] = {
      ssc.sparkContext.broadcast(DISSink[Array[Byte], String](params))
    }
    var i = 0
    while (true)
    {
      // Make RDD
      val list = List("Hello Structured Streaming Sink " + i)
      val listRDD = ssc.sparkContext.makeRDD(list)
      val result = listRDD.map(x => x)

      // Send to DIS
      println(result.first())
      val metadata:Future[RecordMetadata] = disProducer.value.send(streamName, result.first())
      metadata.get()
      i = i + 1

      Thread.sleep(1000)
    }

    //start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}