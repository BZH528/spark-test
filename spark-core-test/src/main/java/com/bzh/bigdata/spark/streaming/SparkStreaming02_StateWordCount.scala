package com.bzh.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming02_StateWordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")


    // 每3秒一个采集周期采集一次数据
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 设置检查点
    ssc.checkpoint("ssc_checkpoint")

    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.224", 9997)

    val wordDStream: DStream[(String, Int)] = inputDStream.flatMap(_.split(" ")).map((_,1))

    // 第一个参数表示相同key的value值,比如很多个1
    // 第二个参数表示缓冲区相同key的value值
    val resultDStream: DStream[(String, Int)] = wordDStream.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )

    resultDStream.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
