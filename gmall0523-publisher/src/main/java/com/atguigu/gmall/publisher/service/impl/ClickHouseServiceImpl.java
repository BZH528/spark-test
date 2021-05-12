package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.mapper.OrderWideMapper;
import com.atguigu.gmall.publisher.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/11 17:01
 * @Description:
 */
@Service
public class ClickHouseServiceImpl implements ClickHouseService {

    @Autowired
    OrderWideMapper orderWideMapper;

    @Override
    public BigDecimal getOrderAmount(String date) {
        return orderWideMapper.selectOrderAmountTotal(date);
    }

    /**
     * List<Map{hr->11,amount->1000}>  ==》 Map{11-> 1000}
     * @param date
     * @return
     */
    @Override
    public Map<String, BigDecimal> getOrderAmountHour(String date) {
        Map<String, BigDecimal> rsMap = new HashMap<>();
        List<Map> mapList = orderWideMapper.selectOrderAmountHour(date);
        for (Map map : mapList) {
            // 注意：key的名称不能随便写，和mapper映射文件，查询语句的别名一致
            rsMap.put(String.format("%02d", map.get("hr")), (BigDecimal) map.get("am"));
        }

        return rsMap;
    }
}
