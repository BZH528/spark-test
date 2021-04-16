package com.bzh.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming03_Transform {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

    // 每3秒采集一次数据
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.224", 9997)

    // transform可以将底层的RDD获取到后操作
    // transform使用场景 1、DStream功能不完善 2、需要代码周期性执行

    // Code:执行在Driver端
    val dstream: DStream[String] = inputDStream.transform(
      rdd => {
        rdd.map(
          // Code:执行在Driver端 （一个采集周期一个RDD，周期性执行）
          str => {
            // Code:执行在Executor端
            str
          }
        )
      })

    // Code:执行在Driver端
    val dstream1: DStream[String] = inputDStream.map(
      str => {
        // Code:执行在Executor端
        str
      })


    ssc.start()

    ssc.awaitTermination()
  }

}
