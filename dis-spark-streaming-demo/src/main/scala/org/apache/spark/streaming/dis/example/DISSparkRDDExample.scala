package org.apache.spark.streaming.dis.example

import org.apache.spark.streaming.dis.DISUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object DISSparkRDDExample {
  def main(args: Array[String]): Unit = {
    println("Start DIS Spark RDD demo.")
    if (args.length < 7) {
      println(s"args is wrong, should be [endpoint region ak sk projectId streamName offsetRanges]".stripMargin)
      return
    }

    // DIS GW url
    // Region ID
    // Access Key Id
    // Secret Access Key
    // User ProjectId
    // DIS stream name
    // OffsetRanges: Use json format to specify the offsetRanges of each partition
    //               SYNOPSIS: "{\"partitionNum1\": [fromOffset1, untilOffset1], \"partitionNum2\": [fromOffset2, untilOffset2], ...}"  (-1 indicates the latest, -2 indicates the earliest)
    //               eg. "{\"0\": [100, 200], \"1\": [-2, -1], \"2\": [100, -1]}"
    val (endpoint, region, ak, sk, projectId, streamName, offsetRanges)
    = (args(0), args(1), args(2), args(3), args(4), args(5), args(6))

    val sparkConf = new SparkConf().setAppName("DIS Spark Rdd example")
    val sc = new SparkContext(sparkConf)

    val params = Map[String, Object](
      DISUtils.PROPERTY_ENDPOINT -> endpoint,
      DISUtils.PROPERTY_REGION_ID -> region,
      DISUtils.PROPERTY_AK -> ak,
      DISUtils.PROPERTY_SK -> sk,
      DISUtils.PROPERTY_PROJECT_ID -> projectId)

    val rdd = DISUtils.createRDD[String, String](sc, params.asJava, streamName, offsetRanges)

    // print all record
    rdd.map(kv => kv.topic() + " " + kv.partition() + " " + kv.offset() + " " + kv.key() + " " + kv.value()).foreach(k => println(k))
  }
}