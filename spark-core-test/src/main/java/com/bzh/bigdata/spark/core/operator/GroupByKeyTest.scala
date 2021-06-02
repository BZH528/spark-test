package com.bzh.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyTest {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(sparkConf)

    val numRDD = sc.makeRDD(List(("a",1), ("a",2), ("b",3), ("a",4), ("b",5), ("a",6)))

//    val outputRDD: RDD[(String, Iterable[Int])] = numRDD.groupByKey()

    val reduceByKeyRDD: RDD[(String, Int)] = numRDD.reduceByKey((a,b) => b)


//    outputRDD.collect().foreach(println)
    reduceByKeyRDD.collect().foreach(println)



    sc.stop()

  }
}
