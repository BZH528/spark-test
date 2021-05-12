package com.gmall.realtime.util

import java.sql._

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/5/7 16:36
  * @Description: 查询 phoenix 工具类
  */
object PhoenixUtil {

  def queryList(sql:String):List[JSONObject] = {
    //注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val resultList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]

    // 建立连接
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:node02:2181")

    // 创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    // 执行sql语句
    val rs: ResultSet = ps.executeQuery()
    val rsMetaData: ResultSetMetaData = rs.getMetaData
    //处理结果集
    while (rs.next()) {
      val userStatusObj: JSONObject = new JSONObject()

      for (i <- 1 to rsMetaData.getColumnCount) {
        userStatusObj.put(rsMetaData.getColumnName(i), rs.getObject(i))
      }
      resultList.append(userStatusObj)
    }

    // 释放资源
    rs.close()
    ps.close()
    conn.close()

    resultList.toList
  }
}
