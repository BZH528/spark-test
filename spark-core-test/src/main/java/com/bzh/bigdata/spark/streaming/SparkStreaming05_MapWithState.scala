package com.bzh.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object SparkStreaming05_MapWithState {

  def main(args: Array[String]): Unit = {

    // windows运行hadoop
    System.load("D:\\bzh\\guojiaorunwan\\tools\\winutils-master\\hadoop-3.0.0\\bin\\hadoop.dll");//建议采用绝对地址，bin目录下的hadoop.dll文件路径




    // 每3秒一个采集周期采集一次数据
    val ssc: StreamingContext = StreamingContext.getOrCreate("ssc_checkpoint/mapwithstate2", getStreamingContext _)




    ssc.start()

    ssc.awaitTermination()
  }

  def getStreamingContext:StreamingContext = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming05_MapWithState")


    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    // 设置检查点
    ssc.checkpoint("ssc_checkpoint/mapwithstate2")

    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.224", 9997)

    val wordDStream: DStream[(String, Int)] = inputDStream.flatMap(_.split(" ")).map((_,1))


    /**
      * 定义一个函数，该函数有三个类型word: String, one: Option[Int], state: State[Int]
      * 其中word代表统计的单词，one代表的是历史数据（使用option是因为历史数据可能有，也可能没有，如第一次进来的数据就没有历史记录），state代表的是返回的状态
      */
    val mappingFunc = (word:String, one:Option[Int], state:State[Int]) => {
      if (state.isTimingOut()) {
        println(word + " is timeout")
      } else {
        val sum = one.getOrElse(0) + state.getOption().getOrElse(0)
        val output = (word, sum)
        state.update(sum)
        output
      }

    }
    val resultDStream: MapWithStateDStream[String, Int, Int, Any] = wordDStream.mapWithState(StateSpec.function(mappingFunc).timeout(Seconds(30)))

    resultDStream.print()

    ssc
  }

}
