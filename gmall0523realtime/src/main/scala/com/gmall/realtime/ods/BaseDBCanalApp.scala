package com.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/5/6 15:50
  * @Description: 从kafka读取数据根据表名进行分流处理
  */
object BaseDBCanalApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("BaseDBCanalApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    var topic = "gmall0523_db_c"
    var groupId = "base_db_cannl_group"

    // 从redis中获取偏移量
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null

    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    if (offsetMap != null && offsetMap.size > 0) {
      // 从指定位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      // 从最新位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 获取当前批次读取的kafka主题的偏移量信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // 对接收到的数据进行结构的转换 ConsumerRecord[String, String(jsonStr)] ==> jsonObj
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
        // 将json字符串转为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }

    // 分流，根据不同的表名，将数据发送到不同kafka主题中
    jsonObjDStream.foreachRDD{
      rdd => {
        rdd.foreach{
          jsonObj => {
            val opType: String = jsonObj.getString("type")
            if ("INSERT".equals(opType) || "UPDATE".equals(opType)) {
              // 获取表名
              val table: String = jsonObj.getString("table")
              // 获取操作数据
              val dataArr: JSONArray = jsonObj.getJSONArray("data")
              // 拼接目标topic名称
              var sendTopic = "ods_" + table

              //为适应小金盒ibox项目改动，将此处注释
              // 对dataArr数组遍历
              /*import scala.collection.JavaConverters._
              for (data <- dataArr.asScala) {
                // 根据表名将数据发送到不同主题
                MyKafkaSink.send(sendTopic, data.toString)
              }*/

              //为适应小金盒ibox项目改动，将此处注释
              MyKafkaSink.send(sendTopic, jsonObj.toString())

            }

          }
        }

        // 提交偏移量
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
