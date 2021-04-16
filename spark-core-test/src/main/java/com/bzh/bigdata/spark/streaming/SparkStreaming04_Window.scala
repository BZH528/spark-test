package com.bzh.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_Window {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

    // 每3秒采集一次数据
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))



    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.224", 9997)

    // 第一个参数windowDuration：窗口长度（包含多少个采集周期后执行计算）
    // 第二个参数slideDuration：窗口之间的滑动的间隔，默认是一个采集周期，此时会重复计算数据，如果间隔刚好是窗口长度的话，则不会重复计算
    val windowDStream: DStream[String] = inputDStream.window(Seconds(6),Seconds(6))

    val resultDStream: DStream[(String, Int)] = windowDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    resultDStream.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
