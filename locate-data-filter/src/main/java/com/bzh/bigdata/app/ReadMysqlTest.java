package com.bzh.bigdata.app;

import com.bzh.bigdata.util.MyPropertiesUtil;
import com.bzh.bigdata.util.MysqlUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/19 16:12
 * @Description:
 */
public class ReadMysqlTest {

    public static void main(String[] args) {

        List<String> logintitudes = new ArrayList<>();
        List<String> latitudes = new ArrayList<>();
        Properties properties = MyPropertiesUtil.load("config.properties");
        String sqlfile_path = properties.getProperty("data.highway.sql.str");
        // 获取mysql库表中经纬度的集合
        MysqlUtils.getHighWayInfoFromMysql(sqlfile_path, logintitudes, latitudes);

        for (String logintitude : logintitudes) {
            System.out.println(logintitude);
        }

    }
}
