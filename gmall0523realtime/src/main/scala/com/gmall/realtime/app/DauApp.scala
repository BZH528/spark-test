package com.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gmall.realtime.bean.DauInfo
import com.gmall.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/4/28 17:21
  * @Description: 日活用户统计业务类
  */
object DauApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("DauApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    var topic = "gmall_start_0523"
    var groupId = "gmall_dau_0523"

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    // 从redis中获取kafka分区的偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
    if (offsetMap != null && offsetMap.size != 0) {
      // 如果redis中存在当前消费者组消费主题的偏移量信息，从指定偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      // 如果redis中没有当前消费者组消费主题的偏移量信息，还是按照配置，从最新位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 获取当前采集周期从kafka消费数据的起始偏移量和结束偏移量值
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetDstream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        // 因为recordDStream底层封装的是KafkaRDD， 混入了HasOffsetRanges特质，这个特质提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    }


    val jsonObjectDstream: DStream[JSONObject] = offsetDstream.map {
      record => {
        val jsonStr: String = record.value()
        // 转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        // 获取事件戳
        val ts: lang.Long = jsonObj.getLong("ts")
        // 时间格式转换
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateStrArr: Array[String] = dateStr.split(" ")
        var dt = dateStrArr(0)
        var hr = dateStrArr(1)
        jsonObj.put("dt", dt)
        jsonObj.put("hr", hr)
        jsonObj
      }
    }
//    jsonObjectDstream.print(1000)

    // 通过redis 对采集到的启动日志进行去重操作 方案2 以分区数据为单位对数据处理，每一个分区获取一次redis连接
    // redis类型 set    key:  dau:2021-04-28  value:  mid   expire:3600 * 24
    val filterDStream: DStream[JSONObject] = jsonObjectDstream.mapPartitions{
      jsonObjItr => {// 以分区为单位处理

        // 每一个分区获取一个redis连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        // 定义一个集合，用于存放当前分区中第一次登陆的日志
        val listBuffer: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()

        // 对分区的数据进行遍历
        for (jsonObj <- jsonObjItr) {
          // 获取日志
          val dt: String = jsonObj.getString("dt")
          // 获取设备id
          val mid: String = jsonObj.getJSONObject("common").getString("mid")
          // 拼接操作redis的key
          var dauKey = "dau:" + dt
          val isFirst = jedis.sadd(dauKey, mid)
          if (isFirst == 1L) {// 集合中不存在这个元素
            // 第一次登陆
            listBuffer.append(jsonObj)
          }
          // 设置key的失效时间
          if (jedis.ttl(dauKey) < 0) {
            jedis.expire(dauKey, 3600 * 24)
          }

        }

        jedis.close()
        listBuffer.toIterator
      }
    }

    //filterDStream.count().print()

    // 将数据批量保存到ES中
    filterDStream.foreachRDD{

      rdd => {
        // 以分区为单位处理
        rdd.foreachPartition {
          jsonObjItr => {
            val dauInfoList: List[(String,DauInfo)] = jsonObjItr.map {
              jsonObj => {
                //每次处理的是一个 json 对象 将 json 对象封装为样例类
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo: DauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00", //分钟我们前面没有转换，默认 00
                  jsonObj.getLong("ts")
                )
                // (es文档id,封装对象)
                (dauInfo.mid, dauInfo)

              }
            }.toList

            // 将数据批量保存到ES中
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfoList, "gmall0523_dau_info_" + dt)

          }
        }

          // 提交偏移量到redis中
        OffsetManagerUtil.saveOffset(topic, groupId, offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
