package com.bzh.bigdata.app;

import com.bzh.bigdata.util.LocateUtils;
import com.bzh.bigdata.util.MyPropertiesUtil;
import com.bzh.bigdata.util.MysqlUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/19 20:15
 * @Description:
 */
public class TestInHighWay {

    public static void main(String[] args) {

        List<String> logintitudes = new ArrayList<>();
        List<String> latitudes = new ArrayList<>();
        Properties properties = MyPropertiesUtil.load("config.properties");
        String sqlfile_path = properties.getProperty("data.highway.sqlfile");
        Double max_distance = Double.valueOf(properties.getProperty("data.highway.compute.distance"));
        // 获取mysql库表中经纬度的集合
        MysqlUtils.getHighWayInfoFromMysql(sqlfile_path, logintitudes, latitudes);

        boolean inHighWay = TestInHighWay.isInHighWay("113481080", "23203660", logintitudes, latitudes, max_distance);
        System.out.println(inHighWay);

    }

    //判断是否是在高速路上
    private static boolean isInHighWay(String target_longtitude, String target_latitude, List<String> longitudes, List<String> latitudes, double max_distance) {
        return LocateUtils.isInHighWayAccordLocate(target_longtitude, target_latitude, longitudes, latitudes);
    }
}
