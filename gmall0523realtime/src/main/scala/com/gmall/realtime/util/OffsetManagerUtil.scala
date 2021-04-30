package com.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/4/29 15:37
  * @Description: 维护偏移量的工具类
  */
object OffsetManagerUtil {



  // 从redis中获取偏移量 type:hash   key:  offset:topic:groupId    field: partition  value: 偏移量
  def getOffset(topic:String, groupId:String):Map[TopicPartition,Long] = {
    // 获取客户端连接
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    // 拼接redis的key offset:topic:groupId
    var offsetKey = "offset:" + topic + ":" + groupId
    // 获取当前消费者组消费主题 对应的分区和偏移量
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)

    // 关闭连接
    jedis.close()

    // 将Java的map转换为scala的map
    import scala.collection.JavaConverters._
    val map: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        println("读取分区偏移量：" + partition + ":" + offset)
        // Map[TopicPartition,Long]
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap
    map

  }

  // 提交偏移量到redis中
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    // 拼接redis中操作偏移量的key
    var offsetKey = "offset:" + topic + ":" + groupId
    // 定义java的Map集合，用于存放每个分区的偏移量
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String,String]()
    // 对offsetRanges进行遍历，将数据封装为offsetMap
    for (offsetRange <- offsetRanges) {
      val partition: Int = offsetRange.partition
      val fromOffset: Long = offsetRange.fromOffset
      val untilOffset: Long = offsetRange.untilOffset
      offsetMap.put(partition.toString, untilOffset.toString)
      println("保存分区" + partition + ":" + fromOffset + " ===>" + untilOffset)
    }

    val jedis: Jedis = MyRedisUtil.getJedisClient()
    jedis.hmset(offsetKey, offsetMap)
    jedis.close()

  }

}
