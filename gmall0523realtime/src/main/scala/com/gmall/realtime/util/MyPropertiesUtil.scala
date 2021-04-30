package com.gmall.realtime.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/4/28 9:51
  * @Description: 读取配置文件
  */
object MyPropertiesUtil {

  /*def main(args: Array[String]): Unit = {

    val properties: Properties = MyProperties.load("config.properties")
    val res: String = properties.getProperty("kafka.broker.list")
    println(res)
  }*/


  /**
  * @Description: 读取配置文件
  * @Param: [propertisName]
  * @return: java.util.Properties
  * @Author: bizh
  * @Date: 2021/4/28
  */
  def load(propertisName:String): Properties = {

    val properties: Properties = new Properties()

    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertisName), StandardCharsets.UTF_8))

    properties
  }

}
