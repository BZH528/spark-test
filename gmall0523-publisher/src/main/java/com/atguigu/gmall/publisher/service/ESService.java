package com.atguigu.gmall.publisher.service;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/4/29 19:16
 * @Description: 操作ES接口
 */
public interface ESService {

    // 查询某天的日活数
     Long getDauTotal(String date);

     // 分时统计某天日活数
     Map<String, Long> getDauHour(String date);
}
