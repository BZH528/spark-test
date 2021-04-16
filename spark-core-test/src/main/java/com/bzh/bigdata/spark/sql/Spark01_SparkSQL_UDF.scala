package com.bzh.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_UDF {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark01_SparkSQL")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // DataFrame
    val df: DataFrame = spark.read.json("datas/user.json")

    df.createOrReplaceTempView("user")

    // 用户自定义函数
    spark.udf.register("prefixName", (name:String) => {
      "Name:" + name
    })

    val newDF: DataFrame = spark.sql("select age, prefixName(username) from user")

    newDF.show()

    spark.close()
  }

  case class  User(id:Int, name:String, age:Int)

}
