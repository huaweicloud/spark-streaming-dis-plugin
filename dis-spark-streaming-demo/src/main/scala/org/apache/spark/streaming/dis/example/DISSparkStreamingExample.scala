package org.apache.spark.streaming.dis.example

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dis.{ConsumerStrategies, DISUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DISSparkStreamingExample {
  def main(args: Array[String]): Unit = {
    println("Start DIS Spark Streaming demo.")
    if (args.length < 8) {
      println(s"args is wrong, should be [endpoint region ak sk projectId streamName startingOffsets duration]".stripMargin)
      return
    }
    
    // DIS GW url
    // Region ID
    // Access Key Id
    // Secret Access Key
    // User ProjectId
    // DIS stream name
    // Starting offsets:  'LATEST'    (Starting with the latest sequenceNumber) 
    //                    'EARLIEST'  (Starting with the earliest sequenceNumber)
    //                    '{"0":23,"1":-1,"2":-2}'  (Use json format to specify the starting sequenceNumber of each partition, -1 indicates the latest, -2 indicates the earliest)
    // Duration: StreamingContext duration(second)
    val (endpoint, region, ak, sk, projectId, streamName, startingOffsets, duration)
    = (args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))

    val sparkConf = new SparkConf().setAppName("Spark streaming DIS example")
    val ssc = new StreamingContext(sparkConf, Seconds(duration.toInt))

    val params = Map(
      DISUtils.PROPERTY_ENDPOINT -> endpoint,
      DISUtils.PROPERTY_REGION_ID -> region,
      DISUtils.PROPERTY_AK -> ak,
      DISUtils.PROPERTY_SK -> sk,
      DISUtils.PROPERTY_PROJECT_ID -> projectId)
    val stream = DISUtils.createDirectStream[String, String](
      ssc,
      ConsumerStrategies.Assign[String, String](streamName, params, startingOffsets))

    // word count
    stream.map(_.value).flatMap(_.split(" ")).map(x => (x, 1L)).reduceByKey(_ + _).print()

    //start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}