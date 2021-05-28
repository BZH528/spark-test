package com.bzh.bigdata.xiaojinhe

import java.security.PrivilegedAction

import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.{LongColumnInterpreter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.hbase.TableName


/**
  * Created by Administrator on 2019/11/15 0015.
  */
object Hbase {



  /*def getRowCount(table:String):Long={
    var count=0L
    var admin:Admin =null
    var aggregationClient:AggregationClient=null
    try{
      admin = HbaseUtils.getHbaseAdmin
      val name:TableName = TableName.valueOf(table)
      val descriptor:HTableDescriptor = admin.getTableDescriptor(name)
      val coprocessorClass:String = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation"
      if(!descriptor.hasCoprocessor(coprocessorClass)){
        admin.disableTable(name);
        descriptor.addCoprocessor(coprocessorClass)
        admin.modifyTable(name, descriptor)
        admin.enableTable(name)
      }
      val scan:Scan = new Scan()
      scan.setTimeRange(NumberUtils.toLong(Util.getDayStartTime()), NumberUtils.toLong(Util.getDayEndTime()))
      aggregationClient = HbaseUtils.getAggregationClient
      count = aggregationClient.rowCount(name, new LongColumnInterpreter(), scan)
    }catch{
      case e:Exception =>e.printStackTrace()
    }
    return  count
  }*/


 def getValue(tableName:String,key:String):String={
   val table:Table = HbaseUtils.getTable(TableName.valueOf(tableName))
   var str="";
   try{
     val result = table.get(new Get(Bytes.toBytes(key)))
     if(!result.isEmpty){
       str = Bytes.toString(result.getValue(Bytes.toBytes("non"),Bytes.toBytes("value")))
     }
   }catch {
     case e:Exception=>e.printStackTrace()
   }
   str
 }

  def putValue(tableName:String,key:String,value:String)={
    val table:Table = HbaseUtils.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(key))
//    put.add(Bytes.toBytes("non"),Bytes.toBytes("value"),Bytes.toBytes(value))
    put.addColumn(Bytes.toBytes("non"), Bytes.toBytes("value"), Bytes.toBytes(value))
    table.put(put)
    table.close()
  }






  def getNewUserList(userMap:scala.collection.mutable.HashMap[String,String] , tableName:String)={
    var userList:List[String] = List()
    var getList: List[Get] = List()
    var reList:List[(String,Long)] = List()
    val table:Table = HbaseUtils.getTable(TableName.valueOf(tableName))
    try{
      for(entry<-userMap){
        userList = userList:+entry._1
        getList = getList:+new Get(Bytes.toBytes(entry._1))
      }
      import scala.collection.JavaConverters._
      val results:Array[Result] = table.get(getList.asJava)
      for(i<-0 until results.length) {
        if (results(i).isEmpty) {
          val rowkey:String = userList(i)
          reList = reList:+(userMap.get(rowkey).get,1L)
        }
      }
    }catch {
      case e:Exception=>e.printStackTrace()
    }

    reList

  }


  def getHBaseConn(): Connection = {
    val configuration = HBaseConfiguration.create
    configuration.set("hbase.zookeeper.quorum",ConfigFactory.hbasezookeeper)
    ConnectionFactory.createConnection(configuration)
  }


  /*def getKerberosConfiguration(principal: String, keytabPath: String,table:String)= {
    val configuration = HBaseConfiguration.create
    configuration.addResource(new Path(ConfigFactory.confPath +"hbase-conf/core-site.xml"))
    configuration.addResource(new Path(ConfigFactory.confPath +"hbase-conf/hdfs-site.xml"))
    configuration.addResource(new Path(ConfigFactory.confPath +"hbase-conf/hbase-site.xml"))
    UserGroupInformation.setConfiguration(configuration)
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath)
    val loginUser = UserGroupInformation.getLoginUser
    loginUser.doAs(new PrivilegedAction[Configuration] {
      override def run()={
        configuration.set(TableOutputFormat.OUTPUT_TABLE, table)
        val job = Job.getInstance(configuration)
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.getConfiguration
      }
    })
  }*/

  /*def main(args: Array[String]): Unit = {
    /*val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",ConfigFactory.hbasezookeeper)
    val connection: Connection = ConnectionFactory.createConnection(conf)


    val table: Table = connection.getTable(TableName.valueOf("tmp:cacheTable_2021-05-28"))
    val put: Put = new Put(Bytes.toBytes("rk110"))

    put.addColumn(Bytes.toBytes("non"), Bytes.toBytes("value"), Bytes.toBytes("123"))
    table.put(put)
    table.close()
    println("插入数据成功")*/


   putValue("tmp:cacheTable_" + Util.getDate(), "rk222", "1")
  }*/

}
