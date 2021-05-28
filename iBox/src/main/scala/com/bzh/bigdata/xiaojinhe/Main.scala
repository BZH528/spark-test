package com.bzh.bigdata.xiaojinhe

import java.lang

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/5/17 9:26
  * @Description: SparkStreaming实时计算订单指标和日志指标
  */
object Main {

  private val logger: Logger = LoggerFactory.getLogger(Main.getClass)



  def main(args: Array[String]): Unit = {
    // windows运行hadoop
    System.load("D:\\bzh\\guojiaorunwan\\tools\\winutils-master\\hadoop-3.0.0\\bin\\hadoop.dll");//建议采用绝对地址，bin目录下的hadoop.dll文件路径

    startJob(args(0))


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


    // 本地测试，设置本地master
    conf.setMaster("local[4]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext


    while (true) {
      /**
        * 让你的 application 能从 driver 失败中恢复，你的 application 要满足
        * 若 application 为首次重启，将创建一个新的 StreamContext 实例
        * 如果 application 是从失败中重启，将会从 checkpoint 目录导入 checkpoint 数据来重新创建 StreamingContext 实例
        * 通过 StreamingContext.getOrCreate 可以达到目的
        */
      val ssc: StreamingContext = StreamingContext.getOrCreate(ConfigFactory.checkpointdir + Util.getDate(), getStreamingContext _ )
//      Thread.sleep(300000)
      ssc.start()

      /**
        * 执行后会等待任务的结束，如果结束会返回 True，如果过程中出现异常会抛出，可以设置 timeout 时长，设置后如果限定时长内没有等到任务结束，就返回 False。
          所以不难看出，我们只要设置 timeout 时长至每天的零点，然后等待该方法返回 False 再去重新 start 任务就好了。因为返回 False 就意味着这个方法一直等到了零点任务都没有结束
        */
//      ssc.awaitTerminationOrTimeout(Util.resetTime)

      ssc.awaitTermination()
      /**
        * 第一个true：停止相关的SparkContext。无论这个流媒体上下文是否已经启动，底层的SparkContext都将被停止。
        * 第二个true：则通过等待所有接收到的数据的处理完成，从而优雅地停止
        */
//      ssc.stop(false,true)
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

    /*  val valueDStream: DStream[String] = stream.map(_.value())
      valueDStream.print(100)*/


      // 获取当前批次读取的kafka主题的偏移量信息
      var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
      val offsetDStream: DStream[ConsumerRecord[String, String]] = stream.transform {
        rdd => {
          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      }
      val filterDStream: DStream[ConsumerRecord[String, String]] = offsetDStream.filter(item => orderFilterFunction(item, topics(1)))

//      val filterWithValueDStream: DStream[String] = filterDStream.map(_.value())
//      filterWithValueDStream.print(100)


      // 计算订单量和订单金额
      val orderStatisticDStream: DStream[(String, (Long, Double))] = filterDStream.map {
        x => {
          val json: JSONObject = JSON.parseObject(x.value())
          val data: JSONObject = json.getJSONArray("data").getJSONObject(0)
          val money: Double = data.getOrDefault("final_total_amount", "0.0").toString.toDouble
          ("order", (1L, money))
        }
      }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

//      orderStatisticDStream.print()

      // 将累计状态写出到hbase，每个周期的结果写出到mysql
      orderStatisticDStream.foreachRDD{
        rdd => {
          rdd.persist(StorageLevel.MEMORY_AND_DISK)
          rdd.collect().foreach(x => orderStatisticUpdateAndWriteToMysql(x))

          // 等输出操作完成后提交offset
          stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }
      }
      /*stream.foreachRDD{
        rdd => {
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd.persist(StorageLevel.MEMORY_AND_DISK)
          rdd.filter(item => orderFilterFunction(item, topics(1)))
            .map(
              x => {
                val json: JSONObject = JSON.parseObject(x.value())
                val data: JSONObject = json.getJSONArray("data").getJSONObject(0)
                val money: Double = data.getOrDefault("final_total_amount", "0.0").toString.toDouble
                ("order", (1L,money))
              }
            )
            .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
            .collect()
            .foreach(x => orderStatisticUpdateAndWriteToMysql(x))



          // 等输出操作完成后提交offset
          stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }
      }*/

      ssc
    }
  }



  def orderFilterFunction(item: ConsumerRecord[String, String], topic: String): Boolean = {
    logger.warn("msg:=============================\r\n" + item.value())

    if (item.topic().equals(topic)) {
      val json: JSONObject = JSON.parseObject(item.value())
      val process_type: String = json.getString("type")
      val data: JSONObject = json.getJSONArray("data").getJSONObject(0)
      if (process_type.equals("INSERT")) {

          // pay_sucess:1004,order_success:1005
          val order_status: String = data.getString("order_status").toLowerCase()
          val suc = order_status.equals("1005") || order_status.equals("1004")
          suc


      } else if (process_type.equals("UPDATE")) {
        val old: JSONObject = json.getJSONArray("old").getJSONObject(0)
        if (!old.getString("order_status").toLowerCase().equals("1004") && data.getString("order_status").equals("1004")) {
          true
        } else {
          false
        }
      } else {
        false
      }
    } else {
      false
    }


  }

  def orderStatisticUpdateAndWriteToMysql(x: (String, (Long, Double))):Unit = {
    logger.warn("to insert or update data:" + x)
    val now: String = Util.getFormatTime
    val old_order_count: String = Hbase.getValue("tmp:cacheTable_" + Util.getDate(), "order_count")
    var result_order_count = 0L
    if (!old_order_count.equals("")) {
        result_order_count = old_order_count.toLong
    }
    val new_order_count = result_order_count + x._2._1
    Hbase.putValue("tmp:cacheTable_" + Util.getDate(), "order_count", new_order_count.toString)
    Mysql.writeToMysql(x._1, "order_count", String.valueOf(new_order_count), now)

    val old_order_account = Hbase.getValue("tmp:cacheTable_" + Util.getDate(), "order_account")
    var result_order_account = 0.0
    if (!old_order_account.equals("")) {
      result_order_account = old_order_account.toDouble
    }
    val new_order_account = result_order_account + x._2._2
    Hbase.putValue("tmp:cacheTable_" + Util.getDate(), "order_account", new_order_account.toString)
    Mysql.writeToMysql(x._1, "order_account", String.valueOf(new_order_account), now)
  }



}
