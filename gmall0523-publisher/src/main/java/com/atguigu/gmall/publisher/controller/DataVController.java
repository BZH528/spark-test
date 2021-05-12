package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.service.MySQLService;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/12 17:11
 * @Description: 对接 DataV 的 Controller
 */
@RestController
public class DataVController {

    @Autowired
    MySQLService mySQLService;

    @RequestMapping("/trademark-sum")
    public Object trademarkSum(@RequestParam("start_time") String startTime, @RequestParam("end_time") String endTime, @RequestParam("topN") int topN) {
        List<Map> trademarkSumList = mySQLService.getTrademardStat(startTime,
                endTime,topN);
        //根据 DataV 图形数据要求进行调整， x :品牌 ,y 金额， s 1
        List<Map> datavList=new ArrayList<>();
        for (Map trademardSumMap : trademarkSumList) {
            Map map = new HashMap<>();
            map.put("x",trademardSumMap.get("trademark_name"));
            map.put("y",trademardSumMap.get("amount"));
            map.put("s",1);
            datavList.add(map);
        }
        return datavList;

    }
}
