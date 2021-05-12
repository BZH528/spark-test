package com.gmall.realtime.bean

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/5/7 16:06
  * @Description: 用于映射用户状态表的样例类
  */
case class UserStatus(
                       userId:String, //用户 id
                       ifConsumed:String //是否消费过 0 首单 1 非首单
                     )

