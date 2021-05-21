package com.bzh.bigdata.xiaojinhe

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/5/17 9:26
  * @Description: SparkStreaming实时计算订单指标和日志指标
  */
object Main {


  def main(args: Array[String]): Unit = {
        startJob(args(0))
  }

  def orderFilterFunction(item: ConsumerRecord[String, String], topic: String): Boolean = {

    if (item.topic().equals(topic)) {
      val json: JSONObject = JSON.parseObject(item.value())

    }

  }

  def startJob(consumeRate: String): Unit = {
    // 初始化配置文件
    val conf: SparkConf = new SparkConf().setAppName(ConfigFactory.sparkstreamname)
    // 通过GracefulShutDown，首先将Receiver关闭，不再接收新数据，然后将已经收下来的数据处理完，然后再退出，这样checkpoint可以安全删除
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    //控制每秒读取Kafka每个Partition最大消息数(500*3*10=15000)，若Streaming批次为10秒，topic最大分区为3，则每批次最大接收消息数为15000
    conf.set("spark.streaming.kafka.maxRatePerPartition", consumeRate)
     /*该参数用于设置每个stage的默认task数量。这个参数极为重要，如果不设置可能会直接影响你的Spark作业性能。
     因此Spark官网建议的设置原则是，设置该参数为num-executors * executor-cores的2~3倍较为合适，比如Executor的总CPU core数量为300个，那么设置1000个task是可以的，此时可以充分地利用Spark集群的资源
     */
    conf.set("spark.default.parallelism", "30")
    /* 通过动态收集系统的一些数据来自动地适配集群数据处理能力
      如果用户配置了 spark.streaming.receiver.maxRate 或 spark.streaming.kafka.maxRatePerPartition，那么最后到底接收多少数据取决于三者的最小值
    */
    conf.set("spark.streaming.backpressure.enabled", "true")
    // 提交应用程序时使用的暂存目录,存放启动jvm进程的jar包
    conf.set("spark.yarn.stagingDir", "hdfs:///user/spark_works")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    while (true) {
      StreamingContext.getOrCreate(ConfigFactory.checkpointdir + Util.getDate(), getStreamingContext _ )
    }

    def getStreamingContext():StreamingContext = {
      val tmpTableName: String = "tmp:cacheTable_" + Util.getDate()
      val hbaseAdmin: Admin = HbaseUtils.admin

      if (!hbaseAdmin.tableExists(TableName.valueOf(tmpTableName))) {
        HbaseUtils.createNewTable(tmpTableName)
      }

      val ssc: StreamingContext = new StreamingContext(sc, Seconds(ConfigFactory.sparkstreamseconds))
      ssc.checkpoint(ConfigFactory.checkpointdir + Util.getDate())
      val topics: Array[String] = ConfigFactory.kafkatopic

      val kafkaParams: Map[String, Object] = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConfigFactory.kafkaipport,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.GROUP_ID_CONFIG -> ConfigFactory.kafkagroupid,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false:lang.Boolean),
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
      )
      // 读取kafka数据流
      val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

      stream.foreachRDD{
        rdd => {
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd.persist(StorageLevel.MEMORY_AND_DISK)
          rdd.filter(item => orderFilterFunction(item, topics(1)))
        }
      }

      null
    }
  }






}
