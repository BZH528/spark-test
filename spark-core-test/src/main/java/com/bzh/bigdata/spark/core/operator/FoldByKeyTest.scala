package com.bzh.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKeyTest {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(sparkConf)

    val numRDD = sc.makeRDD(List(("a",1), ("a",2), ("b",3), ("a",4), ("a",5), ("b",6)), 2)

    // 分区内求最大值，分区间求和


   // foldbykey针对分区内和分区间的计算规则一样是可以简化aggregatebykey

    val resRDD: RDD[(String, Int)] = numRDD.foldByKey(0)(_+_)

    resRDD.collect().foreach(println)

    sc.stop()

  }
}
