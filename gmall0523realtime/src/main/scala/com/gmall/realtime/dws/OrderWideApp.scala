package com.gmall.realtime.dws

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/5/10 14:54
  * @Description: 从kafka的dwd层，读取订单和订单明细数据
  */
object OrderWideApp {

  def main(args: Array[String]): Unit = {

    //===============1.从Kafka中获取数据================
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderWideApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoTopic = "dwd_order_info"
    val orderInfoGroupId = "dws_order_info_group"

    val orderDetailTopic = "dwd_order_detail"
    val orderDetailGroupId = "dws_order_detail_group"

    //获取偏移量
    val orderInfoOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderInfoTopic,orderInfoGroupId)
    val orderDetailOffsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(orderDetailTopic,orderDetailGroupId)


    var orderInfoRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderInfoOffsetMap!= null && orderInfoOffsetMap.size > 0){
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic,ssc,orderInfoOffsetMap,orderInfoGroupId)
    }else{
      orderInfoRecordDStream = MyKafkaUtil.getKafkaStream(orderInfoTopic,ssc,orderInfoGroupId)
    }

    var orderDetailRecordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(orderDetailOffsetMap!= null && orderDetailOffsetMap.size > 0){
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic,ssc,orderDetailOffsetMap,orderDetailGroupId)
    }else{
      orderDetailRecordDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic,ssc,orderDetailGroupId)
    }


    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoDStream: DStream[ConsumerRecord[String, String]] = orderInfoRecordDStream.transform {
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailDStream: DStream[ConsumerRecord[String, String]] = orderDetailRecordDStream.transform {
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val orderInfoDS: DStream[OrderInfo] = orderInfoDStream.map {
      record => {
        val orderInfoStr: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
        orderInfo
      }
    }


    val orderDetailDS: DStream[OrderDetail] = orderDetailDStream.map {
      record => {
        val orderDetailStr: String = record.value()
        val orderDetail: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
        orderDetail
      }
    }

    //===============2.双流Join================
    //开窗
    val orderInfoWindowDStream: DStream[OrderInfo] = orderInfoDS.window(Seconds(20), Seconds(5))
    val orderDetailWindowDStream: DStream[OrderDetail] = orderDetailDS.window(Seconds(20), Seconds(5))

    // 转为kv结构
    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] = orderInfoWindowDStream.map {
      orderInfo => (orderInfo.id, orderInfo)
    }
    val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] = orderDetailWindowDStream.map {
      orderDetail => (orderDetail.order_id, orderDetail)
    }

    // 双流join
    val joinedDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream)

    //去重 redis type:Set   key:order_join:[orderId]    value:orderDetail   expire:600
    val orderWideDStream: DStream[OrderWide] = joinedDStream.mapPartitions {
      tupleItr => {
        val tupleList: List[(Long, (OrderInfo, OrderDetail))] = tupleItr.toList
        // 获取Jedis客户端
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        val orderWideList: ListBuffer[OrderWide] = new ListBuffer[OrderWide]
        for ((orderId, (orderInfo, orderDetail)) <- tupleList) {
          val orderKey: String = "order_join" + orderId
          val isNotExists: lang.Long = jedis.sadd(orderKey, orderDetail.id.toString)
          jedis.expire(orderKey, 600) // 设置过期时间
          if (isNotExists == 1L) {
            orderWideList.append(new OrderWide(orderInfo, orderDetail))
          }
        }
        jedis.close()
        orderWideList.toIterator
      }
    }
//    orderWideDStream.print(1000)

    //===============3.实付分摊================
    /**
      * 存1:
        明细中 Σ商品价格order_price * 商品数量sku_num
        type:String		 key:	order_origin_sum:[order_id]		value:			expire:	600
      存2：
        明细中 ∑已经计算完成的明细实付分摊金额
        type:String		 key:	order_split_sum:[order_id]		value:			expire:600
      */
    val orderWideSplitDStream: DStream[OrderWide] = orderWideDStream.mapPartitions {
      orderWideItr => {
        val orderWideList: List[OrderWide] = orderWideItr.toList
        // 获取jedis连接
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        for (orderWide <- orderWideList) {
          //3.1 从redis中获取明细累加
          var orderOriginSumKey: String = "order_origin_sum:" + orderWide.order_id
          var orderOriginSum: Double = 0D
          val orderOriginSumStr: String = jedis.get(orderOriginSumKey)
          // 注意：从redis中获取字符串，都要做非空判断
          if (orderOriginSumStr != null && orderOriginSumStr.size > 0) {
            orderOriginSum = orderOriginSumStr.toDouble
          }

          //3.2 从redis中获取实付分摊累加和
          var orderSplitSumKey = "order_split_sum:" + orderWide.order_id
          var orderSplitSum: Double = 0D
          val orderSplitSumStr: String = jedis.get(orderSplitSumKey)
          if (orderSplitSumStr != null && orderSplitSumStr.size > 0) {
            orderSplitSum = orderSplitSumStr.toDouble
          }

          val detailAmount: Double = orderWide.sku_price * orderWide.sku_num

          //3.3 判断是否为最后一条  计算实付分摊金额
          if (detailAmount == orderWide.original_total_amount - orderOriginSum) {
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - orderSplitSum) * 100d) / 100d
          } else {
            orderWide.final_detail_amount = Math.round((orderWide.final_total_amount * detailAmount / orderWide.original_total_amount) * 100d) / 100d
          }

          //3.4 更新redis中的值
          var newOrderOriginSum = orderOriginSum + detailAmount
          jedis.setex(orderOriginSumKey, 600, newOrderOriginSum.toString)

          var newOrderSplitSum = orderSplitSum + orderWide.final_detail_amount
          jedis.setex(orderSplitSumKey, 600, newOrderSplitSum.toString)
        }
        //关闭连接
        jedis.close()
        orderWideList.toIterator
      }
    }
//    orderWideSplitDStream.print(1000)
    /** 注意：如果程序数据来源是kafka，那么在进行分叉处理（行动算子触发的），应该进行缓存*/
//    orderWideSplitDStream.cache()
    // 向ClickHouse中保存数据
    val spark: SparkSession = SparkSession.builder().appName("spark_sql_orderWide").getOrCreate()

    // 对DS中的RDD进行处理
    import spark.implicits._
    orderWideSplitDStream.foreachRDD{
      rdd => {

        rdd.cache() // 在进行分叉处理（行动算子触发的），write是行动算子，再写入foreach又是行动算子，应该进行缓存

        val df: DataFrame = rdd.toDF
        df.write.mode(SaveMode.Append)
          .option("batchsize", "100")
          .option("isolationLevel", "NONE") // 设置事务
          .option("numPartitions", "4") // 设置并发
          .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
          .jdbc("jdbc:clickhouse://node01:8123/default","t_order_wide_0523",new Properties())

        // 将数据写回到Kafka dws_order_wide
        rdd.foreach{
          orderWide => {
            MyKafkaSink.send("dws_order_wide", JSON.toJSONString(orderWide,new SerializeConfig(true)))
          }
        }

        //提交偏移量
        OffsetManagerUtil.saveOffset(orderInfoTopic, orderInfoGroupId, orderInfoOffsetRanges)
        OffsetManagerUtil.saveOffset(orderDetailTopic, orderDetailGroupId, orderDetailOffsetRanges)

      }
    }

    ssc.start()
    ssc.awaitTermination()


  }

}
