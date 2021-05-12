package com.atguigu.gmall.publisher.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/11 16:53
 * @Description: clickhouse相关的业务接口
 */
public interface ClickHouseService {

    // 获取指定日期的交易额
    BigDecimal getOrderAmount(String date);

    // 获取指定日期的分时交易额
    /**
     * 从Mapper获取的分时交易格式  List<Map{hr->11,amount->1000}>  ==》 Map{11-> 1000}
     * @param date
     * @return
     */
    Map<String,BigDecimal> getOrderAmountHour(String date);
}
