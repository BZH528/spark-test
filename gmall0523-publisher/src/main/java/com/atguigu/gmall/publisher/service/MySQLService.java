package com.atguigu.gmall.publisher.service;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/12 17:08
 * @Description: 从 ads 层中获取数据提供的服务接口
 */
public interface MySQLService  {

    List<Map> getTrademardStat(String startTime, String endTime, int topN);
}
