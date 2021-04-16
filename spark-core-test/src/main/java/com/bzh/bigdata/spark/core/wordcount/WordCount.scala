package com.bzh.bigdata.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas")

    val words: RDD[String] = lines.flatMap(x => x.split(" "))

    val wordAndOne: RDD[(String, Int)] = words.map(x => (x,1))

    val result: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    result.collect()

    result.foreach(println)


    sc.stop()
  }
}
