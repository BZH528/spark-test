package com.bzh.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/5/17 14:43
  * @Description:
  */
object LastLoginApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("LastLoginApp")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import org.apache.spark.sql.types._
    val schema = StructType(
        StructField("user_id", StringType)::
        StructField("user_name", StringType)::
        StructField("device_code", StringType)::
        StructField("login_time", StringType)::
        Nil
    )

    val userLoginDF: DataFrame = spark.read.schema(schema).csv("data/dt=2020-12-*/")

    val userInfoDF: DataFrame = spark.read.json("data/user.json")



    userLoginDF.createOrReplaceTempView("user_login")
    userInfoDF.createOrReplaceTempView("user_info")

    // 2020-12月最后登录用户的登录信息
    val lastLoginInfoDF = spark.sql(
      """
        |select
        |t.user_id,t.user_name,t.device_code,t.login_time
        |from
        |(
        |select
        |user_id,user_name,device_code,login_time,row_number() over (partition by user_id,user_name order by login_time desc) rank
        |from user_login
        |) t
        |where t.rank = 1
      """.stripMargin)

    lastLoginInfoDF.createOrReplaceTempView("last_login_info")

    // 查询所有用户2020年12月最后登录时间和使用的设备码,没有最后登录时间和使用的设备码保留空值
    val userInfoWithTimeAndDevDF =spark.sql(
      """
        |select
        |t1.user_id ,t1.user_name,t2.login_time last_login_time,t2.device_code last_device_code
        |from user_info t1
        |left join last_login_info t2
        |on t1.user_id = t2.user_id and t1.user_name = t2.user_name
      """.stripMargin)

    userInfoWithTimeAndDevDF.show()

  }

}
