package com.bzh.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyTest {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(sparkConf)

    val numRDD = sc.makeRDD(List(("a",1), ("a",2), ("b",3), ("a",4), ("b",5), ("a",6)), 2)

    // 需求1：分区内求最大值，分区间求和

    // [a,(1,2,3)], [a,(4,5,6)]
    // [a,3], [a, 6]
    // [a, 9]

    // 第一个参数是分区内的函数的初始值，主要用于碰见第一个key和value的时候进行分区内的计算
    // 第二个参数列表传2个参数
    //    第一个是分区内作用的函数
    //    第二个是分区间作用的函数
    val resRDD: RDD[(String, Int)] = numRDD.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )

    resRDD.collect().foreach(println)
//    numRDD.saveAsTextFile("output")

    // 需求2：求相同key的平均值

    // aggregateByKey返回的类型与初始值zeroValue相同
    val newRDD: RDD[(String, (Int, Int))] = numRDD.aggregateByKey((0, 0))(
      (t, v) => (t._1 + v, t._2 + 1),
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)
    )

    // mapValues针对kv类型，只返回values
    val outputRDD: RDD[(String, Double)] = newRDD.mapValues(t => (t._1*1.0)/t._2)

    println("求相同key的平均值")
    outputRDD.collect().foreach(println)

    sc.stop()

  }
}
