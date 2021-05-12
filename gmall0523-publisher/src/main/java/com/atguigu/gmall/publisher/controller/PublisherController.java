package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.service.ClickHouseService;
import com.atguigu.gmall.publisher.service.ESService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/4/29 19:15
 * @Description: 发布数据接口
 */
@RestController
public class PublisherController {

    @Autowired
    ESService esService;

    @Autowired
    ClickHouseService clickHouseService;

    /*
    访问路径：http://publisher:8070/realtime-total?date=2021-04-29
    响应数据:[{"id":"dau","name":"新增日活","value":1200},
            {"id":"new_mid","name":"新增设备","value":233} ]
     */
    // @RequestParam("date") String dt 将请求中date的值复制给dt参数
    @RequestMapping("/realtime-total")
    public  Object realTimeTotal(@RequestParam("date") String dt) {
        List<Map<String, Object>> resList = new ArrayList<>();
        Map<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        Long dauTotal = esService.getDauTotal(dt);

        if (dauTotal == null) {
            dauMap.put("value", 0L);
        } else {
            dauMap.put("value", dauTotal);
        }
        resList.add(dauMap);

        Map<String, Object> midMap = new HashMap<>();
        midMap.put("id", "new_mid");
        midMap.put("name", "新增设备");
        midMap.put("value", 233);
        resList.add(midMap);

        Map<String, Object> orderAmountMap = new HashMap<>();
        orderAmountMap.put("id", "order_amount");
        orderAmountMap.put("name", "新增交易额");
        orderAmountMap.put("value", clickHouseService.getOrderAmount(dt));
        resList.add(orderAmountMap);

        return resList;
    }

    /**
     * 访问路径：http://publisher:8070/realtime-hour?id=dau&date=2019-02-01
     * 响应：{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
     *      "today":{"12":38,"13":1233,"17":123,"19":688 }}
     *
     * 新增需求
     * 访问路径 http://publisher:8070/realtime-hour?id=order_amount&date=2019-02-01
     * 响应   {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
     *        "today":{"12":38,"13":1233,"17":123,"19":688 }}
     *
     * @param id
     * @param dt
     * @return
     *
     */
    // @RequestParam("date") String dt 将请求中date的值复制给dt参数
    @RequestMapping("/realtime-hour")
    public Object realTimeHour(@RequestParam("id") String id, @RequestParam("date") String dt) {

        if ("dau".equals(id)) {
            Map<String, Map<String, Long>> resMap = new HashMap<>();

            //获取今天的日活统计
            Map<String, Long> tdMap = esService.getDauHour(dt);
            resMap.put("today", tdMap);

            //获取昨天的日活统计
            String yd = getYd(dt);
            Map<String, Long> ydMap = esService.getDauHour(yd);
            resMap.put("yesterday", ydMap);

            return resMap;
        } else if ("order_amount".equals(id)) {
            Map<String, Map<String, BigDecimal>> amountMap = new HashMap<>();

            //获取今天的订单总计统计
            Map<String, BigDecimal> tdMap = clickHouseService.getOrderAmountHour(dt);
            amountMap.put("today", tdMap);

            //获取昨天的订单总计统计
            String yd = getYd(dt);
            Map<String, BigDecimal> ydMap = clickHouseService.getOrderAmountHour(yd);
            amountMap.put("yesterday", ydMap);

            return amountMap;
        } else {
            return  null;
        }


    }

    // 获取前一天日期
    private String getYd(String td){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yd = null;
        try {
            Date tdDate = dateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yd = dateFormat.format(ydDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yd;
    }
}
