package com.bzh.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyTest {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(sparkConf)

    val numRDD = sc.makeRDD(List(("a",1), ("a",2), ("b",3), ("a",4), ("b",5), ("a",6)), 2)


    // 需求2：求相同key的平均值

    // combineByKey需要3个参数
    // 第一个参数：将相同key的第一个数据进行结构的转换，实现操作
    // 第二个参数：分区内的计算规则
    // 第三个参数：分区间的计算规则
    val newRDD: RDD[(String, (Int, Int))] = numRDD.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v: Int) => (t._1 + v, t._2 + 1),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    )

    // mapValues针对kv类型，只返回values
    val outputRDD: RDD[(String, Double)] = newRDD.mapValues(t => (t._1*1.0)/t._2)

    println("求相同key的平均值")
    outputRDD.collect().foreach(println)

    sc.stop()

  }
}
