package com.atguigu.gmall.publisher.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/12 17:03
 * @Description: 品牌统计接口
 */
public interface TrademarkStatMapper {

    List<Map> selectTradeSum(@Param("start_time") String startTime, @Param("end_time") String endTime, @Param("topN") int topN);

}
