package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.mapper.TrademarkStatMapper;
import com.atguigu.gmall.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/12 17:09
 * @Description: 从 ads 层中获取数据提供的服务的具体实现
 */
@Service
public class MySQLServiceImpl implements MySQLService {

    @Autowired
    TrademarkStatMapper trademarkStatMapper;

    @Override
    public List<Map> getTrademardStat(String startTime, String endTime, int topN) {
        return trademarkStatMapper.selectTradeSum(startTime, endTime, topN);
    }
}
