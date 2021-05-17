package com.bzh.bigdata.spark.sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/5/13 16:50
  * @Description: 测试SparkSQL去整合hive，前提启动Hive的metastore服务 nohup hive --service metastore 1>/home/bizh/hive_metastore.log 2>&1 &
  */
object AjhwSparkApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("AjhwSparkApp")
      .enableHiveSupport()
      .getOrCreate()




    val userUVDataFrame: DataFrame = spark.sql("select "+ "'"+ args(0) + "'" + "as statis_hour, '首页访问' as indicator_name, '人' as units,cast(count(distinct(id)) as int) as indicator_value from mydb.spm_data where from_unixtime(cast(cast(spm_time as bigint)/1000 as bigint),'yyyy-MM-ddHH:00:00') =" + "'" + args(0) + "'")

//    userUVDataFrame.show()

    val userPVDataFrame: DataFrame = spark.sql("select "+ "'"+ args(0) + "'" + "as statis_hour,'首页访问' as indicator_name, '次' as units,cast(count(*) as int) as indicator_value from mydb.spm_data where from_unixtime(cast(cast(spm_time as bigint)/1000 as bigint),'yyyy-MM-ddHH:00:00') =" + "'" + args(0) + "'")

//    userPVDataFrame.show()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "root")
    connectionProperties.put("driver","com.mysql.jdbc.Driver")

    userUVDataFrame.write.mode(SaveMode.Append).jdbc("jdbc:mysql://node01:3306/txfs?useUnicode=true&characterEncoding=utf-8","ads_ajhw_spm_indicator_hh", connectionProperties)
    userPVDataFrame.write.mode(SaveMode.Append).jdbc("jdbc:mysql://node01:3306/txfs?useUnicode=true&characterEncoding=utf-8","ads_ajhw_spm_indicator_hh", connectionProperties)





    spark.close()
  }

}
