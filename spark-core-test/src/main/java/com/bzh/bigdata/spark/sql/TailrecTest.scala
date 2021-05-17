package com.bzh.bigdata.spark.sql

import scala.annotation.tailrec

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/5/17 16:08
  * @Description:
  */
object TailrecTest {

  def main(args: Array[String]): Unit = {

    val result: Int = sumTailRec(1000)
    println(result)

    val fiboRes: Int = fibonacciTailRec(5)
    println(fiboRes)

    println(fibonacci(5))

  }

  def sumTailRec(n: Int): Int = {

    @tailrec
    def myLoop(acc:Int, n:Int):Int = {
      if(n <= 1) acc
      else myLoop(acc + n, n-1)

    }

    myLoop(1,n)

  }

  def fibonacciTailRec(n:Int) :Int = {

    @tailrec
    def fibonacciLoop(n:Int,acc1:Int,acc2:Int):Int = {
      if (n <= 2) acc2
      else
        fibonacciLoop(n-1,acc2, acc1+acc2)
    }

    fibonacciLoop(n,1,1)

  }

  def fibonacci(n: Int): Int = {
    if (n <= 2)
      1
    else
      fibonacci(n-1) + fibonacci(n-2)
  }

}
