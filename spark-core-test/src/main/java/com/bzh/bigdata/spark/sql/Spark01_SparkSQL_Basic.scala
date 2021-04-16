package com.bzh.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark01_SparkSQL")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // DataFrame
//    val df: DataFrame = spark.read.json("datas/user.json")

    // df.show()

    // DataFrame => sql
   /* df.createOrReplaceTempView("user")

    spark.sql("select * from user").show()
    spark.sql("select count(*) from user").show()
    spark.sql("select avg(age) from user").show()*/

    // DataFrame => DSL
    //df.select("age", "username").show()
    // 在使用DataFrame时，如果涉及到转换操作，需要引人转换规则
    import spark.implicits._
//    df.select($"age" + 1).show()

    // TODO DataSet
//    val seq = Seq(1,2,3,4)
//    val ds: Dataset[Int] = seq.toDS()
//    ds.show()

    // RDD <=> DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"lucy", 30), (2,"tom", 31), (3,"jerry", 32)))

    val df: DataFrame = rdd.toDF("id", "name", "age")

    val newRDD: RDD[Row] = df.rdd

    // DataFrame <=> DataSet
    val ds: Dataset[User] = df.as[User]
    val df2: DataFrame = ds.toDF()

    // RDD <=> DataSet
    val ds2: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()

    val rdd2: RDD[User] = ds2.rdd


    spark.close()
  }

  case class  User(id:Int, name:String, age:Int)

}
