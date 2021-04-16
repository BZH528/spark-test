package com.bzh.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object Spark01_SparkSQL_UDAF {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark01_SparkSQL")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // DataFrame
    val df: DataFrame = spark.read.json("datas/user.json")

    df.createOrReplaceTempView("user")

    // 用户自定义函数
    spark.udf.register("myAvgAge", functions.udaf(new MyAvgAge))

    val newDF: DataFrame = spark.sql("select myAvgAge(age) from user")

    newDF.show()

    spark.close()
  }

  case class  User(id:Int, name:String, age:Int)

  // buff缓存区样例类存放累计值和个数， 默认样例类是val类型参数，改成var
  case class Buff(var total:Long, var count:Long)

  // org.apache.spark.sql.expressions.Aggregator
  // 继承Aggregator
  //重写6个方法
  class MyAvgAge extends Aggregator[Long, Buff, Long] {

    // 初始值
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    //buff：缓冲区
    // in:输入的年龄值
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count = buff.count + 1
      buff
    }

    // 合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }

    // 计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    // 缓存区的编码操作,样例类类型固定是Encoders.product
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出的编码操作，基本类型是scalaXXX
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
