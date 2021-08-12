package com.bzh.bigdata.spark.core

import scala.collection.mutable

object Test {

  def main(args: Array[String]): Unit = {

    println("hello world")

    var userMap = mutable.Map[String,String]()
    userMap.put("1","aa")
    userMap.put("2","bb")
    userMap.put("3","cc")

    val value: String = userMap.get("2").get

    println(value)


  }
}
