package com.bzh.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

    // 每3秒采集一次数据
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.224", 9997)

    val resultDStream: DStream[(String, Int)] = inputDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    resultDStream.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
