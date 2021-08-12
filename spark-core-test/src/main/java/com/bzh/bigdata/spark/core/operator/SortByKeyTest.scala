package com.bzh.bigdata.spark.core.operator



import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object SortByKeyTest {

  /**
    * 第一种方式
    * 调用List的sortBy方法进行排序，然后再调用take取TopN
    */
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(sparkConf)

    val numRDD = sc.makeRDD(List(("a",1), ("a",2), ("b",3), ("a",4), ("b",5), ("a",6)))



    val groupRDD: RDD[(String, Iterable[Int])] = numRDD.groupByKey()
    val sortedRDD: RDD[(String, List[Int])] = groupRDD.mapValues {
      it => {
        val sorted: List[Int] = it.toList.sortBy(x => x).reverse.take(1)
        sorted
      }
    }

    val formatRDD: RDD[(String, String)] = sortedRDD.map(x => (x._1,x._2.mkString(",")))

    formatRDD.collect().foreach(println)

    sc.stop()



  }


  // 使用treeSet
  /*def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(sparkConf)

    val numRDD = sc.makeRDD(List(("a",1), ("a",3), ("b",3), ("a",2), ("b",5), ("a",6)))


    val groupRDD: RDD[(String, Iterable[Int])] = numRDD.groupByKey()
    val topN = 2

    val resultRDD: RDD[(String, List[Int])] = groupRDD.mapValues {
      it => {
        //定义一个可排序的TreeSet集合,treeSet集合默认也有一个隐式参数,默认调用的也是ordering排序规则,为了实现需求,需要我们自定义排序规则

        var treeSet = new mutable.TreeSet[Int]()(new MyOrdering)

        it.foreach {
          x => {
            //将当前取出的每一条数据放入treeSet集合中
            treeSet.add(x)
            //判断treeSet集合的长度,如果大于要求的topN,则移除最小的元素
            if (treeSet.size > topN) {
              val last: Int = treeSet.last
              treeSet -= last
            }
          }
        }

        treeSet.toList

      }
    }



    resultRDD.collect().foreach(println)

    sc.stop()
  }*/

  /*
  在调用reduceBykey的时候传入自定义分区器,自定义分区器,可以将每一个学科都能进入一个分区中,
  每一个分区有且仅有一个学科(reduceBykey默认使用的是HashPartitioner分区器)
   */
  /*def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(sparkConf)

   val numRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",1), ("a",3), ("b",3), ("a",2), ("b",5), ("a",6)))

    val types: Array[String] = numRDD.map(x => x._1).distinct().collect()

    // 重新分区
    val myPartitoner: MyPartitoner = new MyPartitoner(types)

    val partitionedRDD: RDD[(String, Int)] = numRDD.partitionBy(myPartitoner)

    val topN = 1

    //对每个分区进行排序,按照有界优先队列的方式在Treeset集合中进行排序,这样就可以实现每个分区可以在executor端进行并行排序,而且不会造成内存溢出的情况
    val ResultRDD: RDD[(String, Int)] = partitionedRDD.mapPartitions {
      it => {
        //定义一个可排序的TreeSet集合,treeSet集合默认也有一个隐式参数,默认调用的也是ordering排序规则,为了实现需求,需要我们自定义排序规则

        var treeSet = new mutable.TreeSet[(String, Int)]()(new MyOrdering2)
        it.foreach {
          x => {
            treeSet.add(x)
            if (treeSet.size > topN) {
              val last: (String, Int) = treeSet.last
              treeSet -= last
            }
          }
        }

        treeSet.toIterator
      }
    }
    ResultRDD.collect().foreach(println)

    sc.stop()

  }*/

  //注意  该类要放在object 的外面,实现自定义排序
  class MyOrdering extends Ordering[Int] {
    override def compare(x: Int, y: Int): Int = {

      y-x
    }
  }

  //注意  该类要放在object 的外面,实现自定义排序
  class MyOrdering2 extends Ordering[(String, Int)] {
    override def compare(x: (String, Int), y: (String, Int)): Int = {

      y._2-x._2
    }
  }

  class MyPartitoner(types: Array[String]) extends Partitioner {

    private val myMap: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()

    var index = 0

    for (elem <- types) {
      myMap.put(elem, index)
      index += 1
    }

    // 有多少个分区
    override def numPartitions: Int = types.length

    override def getPartition(key: Any): Int = {
      val subject: String = key.asInstanceOf[String]

      val index: Int = myMap.get(subject).get
      index
    }
  }



}
