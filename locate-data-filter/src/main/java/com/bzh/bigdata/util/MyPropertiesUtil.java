package com.bzh.bigdata.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/19 15:31
 * @Description: 读取配置文件
 */
public class MyPropertiesUtil {

    /*public static void main(String[] args) {
        Properties properties = MyPropertiesUtil.load("config.properties");
        String sqlStr = properties.getProperty("data.highway.sqlstring");
        System.out.println(sqlStr);
    }*/

    public static Properties load(String propertisName) {
        Properties properties = new Properties();

        try {
           properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(propertisName), StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("读取配置文件失败！");
        }
        return properties;

    }
}
