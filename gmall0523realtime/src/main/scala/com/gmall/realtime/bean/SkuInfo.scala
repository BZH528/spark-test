package com.gmall.realtime.bean

/**
  * Created with IntelliJ IDEA.
  *
  * @Author: bizh
  * @Date: 2021/5/10 10:40
  * @Description: 商品样例类
  */
case class SkuInfo(id:String ,
                   spu_id:String ,
                   price:String ,
                   sku_name:String ,
                   tm_id:String ,
                   category3_id:String ,
                   create_time:String,
                   var category3_name:String,
                   var spu_name:String,
                   var tm_name:String)

