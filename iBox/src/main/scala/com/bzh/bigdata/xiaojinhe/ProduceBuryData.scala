package com.bzh.bigdata.xiaojinhe

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Properties, Random}

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/5/31 9:54
  * @Description: 向buryData主题发送模拟消息
  */
object ProduceBuryData {

  def main(args: Array[String]): Unit = {

    val brokerList = ConfigFactory.kafkaipport
    val topic = ConfigFactory.kafkatopic(0)
    val properties: Properties = new Properties()
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)

    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](properties)

    val uids: Array[Int] = Array(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
    val channels:Array[String] = Array("aa", "bb", "cc" , "dd", "ee", "ff", "gg", "hh")

    while (true) {

      val random: Random = new Random()
      val uidRandom: Int = random.nextInt(uids.size)
      val channelRandom: Int = random.nextInt(channels.length)
      val time: LocalDateTime = LocalDateTime.now
      val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val formatTimeStr: String = dateTimeFormatter.format(time)

      val jsonObj: JSONObject = new JSONObject()
      jsonObj.put("uid", uids(uidRandom).toString )
      jsonObj.put("channel", channels(channelRandom))
      jsonObj.put("spm_time", formatTimeStr)

      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, uids(uidRandom).toString, jsonObj.toString())
      producer.send(record)

      Thread.sleep(1000)


    }

    producer.close()
  }

}
