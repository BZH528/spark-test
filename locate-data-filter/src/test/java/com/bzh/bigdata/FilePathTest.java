package com.bzh.bigdata;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/20 13:33
 * @Description:
 */
public class FilePathTest {


    public static void main(String[] args) {
        String pathStr = "/root/mapreduce";
        File file = new File(pathStr);
        if (file == null) {
            System.out.println("file对象为空！");
        } else {
            System.out.println("file对象不为空！");
        }

        String userDir = System.getProperty("user.dir");
        System.out.println("user.dir是：" + userDir);
    }
}
