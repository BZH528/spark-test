package com.atguigu.logger.gmalllogger.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * create_time 2021年4月21日11:06:09
 * 接收模拟器生成的数据，并对数据进行处理
 */
//@Controller // 将对象的创建交给Spring容器 方法返回String，默认会当做跳转页面处理
// @RestController = @Controller + @ResponseBody 方法返回Object，会转为json格式字符串进行相应
@RestController
@Slf4j
public class LoggerController {

    // Spring提供对kafka支持
    @Autowired // 将KafkaTemplate注入到Controller中
    KafkaTemplate kafkaTemplate;

    // @RequestMapping("/applog") 把applog交给方法进行处理
    // @RequestBody 从请求体从获取数据
    @RequestMapping("/applog")
    public String applog(@RequestBody  String mockLog) {
//        System.out.println(mockLog);

        // 落盘
        log.info(mockLog);

        // 根据日志类型，发送到kafka不同主题里
        // 将接收到的字符串转换为json对象
        JSONObject jsonObject = JSONObject.parseObject(mockLog);
        JSONObject startJson = jsonObject.getJSONObject("start");

        if (startJson != null) {
            // 启动日志
            kafkaTemplate.send("gmall_start_0523", mockLog);
        } else {
            // 事件日志
            kafkaTemplate.send("gmall_event_0523", mockLog);
        }


        return "sucess";
    }

}
