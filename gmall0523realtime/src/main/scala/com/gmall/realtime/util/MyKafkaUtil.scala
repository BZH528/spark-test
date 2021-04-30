package com.gmall.realtime.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/4/28 10:33
  * @Description: 消费kafka工具类
  */
object MyKafkaUtil {

  private val properties = MyPropertiesUtil.load("config.properties")
  private val broker_list: String = properties.getProperty("kafka.broker.list")

  // kafka消费配置
  var kafkaParam = collection.mutable.Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    // 用于标识消费者属于哪个消费组
    ConsumerConfig.GROUP_ID_CONFIG -> "gmall0523_group",
    //latest 自动重置偏移量为最新的偏移量
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    // 如果是true，则这个消费者的偏移量在后台自动提交，但是kafka宕机容易丢失数据
    // 如果是false，则需要手动维护offset
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
  )

  // 使用默认的消费者组
  def getKafkaStream(topic: String, ssc:StreamingContext): InputDStream[ConsumerRecord[String,String]] = {
    val dstrem = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic), kafkaParam)
    )

    dstrem
  }

  // 指定消费者组
  def getKafkaStream(topic: String,ssc:StreamingContext,groupId:String): InputDStream[ConsumerRecord[String,String]]={
    kafkaParam(ConsumerConfig.GROUP_ID_CONFIG) = groupId
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam ))
    dStream
  }

  // 从指定偏移量读取数据
  def getKafkaStream(topic: String,ssc:StreamingContext,offsets:Map[TopicPartition,Long],groupId:String): InputDStream[ConsumerRecord[String,String]]={
    kafkaParam("group.id")=groupId
    val dStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam,offsets))
    dStream
  }


}
