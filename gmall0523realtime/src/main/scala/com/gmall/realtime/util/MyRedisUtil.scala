package com.gmall.realtime.util

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/4/28 11:21
  * @Description: 获取Jedis客户端的工具类
  */
object MyRedisUtil {
  // 定义一个连接池对象
  private var jedisPool: JedisPool = null

  def build():Unit = {
    val prop: Properties = MyPropertiesUtil.load("config.properties")
    val host: String = prop.getProperty("redis.host")
    val port: String = prop.getProperty("redis.port")
    val password: String = prop.getProperty("redis.password")

    val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig
    jedisPoolConfig.setMaxTotal(100) //最大连接数
    jedisPoolConfig.setMaxIdle(20) // 最大空闲数
    jedisPoolConfig.setMinIdle(20) // 最小空闲数
    jedisPoolConfig.setBlockWhenExhausted(true) // 忙碌时是否等待
    jedisPoolConfig.setMaxWaitMillis(5000) // 忙碌时等待时长
    jedisPoolConfig.setTestOnBorrow(true) // 每次获得连接时进行测试

    jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt,5000,password)
  }

  // 获取Jedis客户端
  def getJedisClient():Jedis = {
    if (jedisPool == null) {
      build()
    }
    jedisPool.getResource
  }

  /*def main(args: Array[String]): Unit = {
    val jedis: Jedis = getJedisClient()
    println(jedis.ping())
    jedis.close()
  }*/



}
