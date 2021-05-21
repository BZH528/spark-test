package com.bzh.bigdata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/21 14:02
 * @Description: 给hdfs文件夹下文件重命名，添加同一后缀
 */
public class RenameHdfsFile {

    // WIndows下防止idea本地执行报错
    static {
        try {
            System.load("D:\\bzh\\guojiaorunwan\\tools\\winutils-master\\hadoop-3.0.0\\bin\\hadoop.dll");//建议采用绝对地址，bin目录下的hadoop.dll文件路径
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://runone01:8020");

        try {
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fileStatuses = fs.listStatus(new Path("/mr/output/shenshanxi/filter-2021-05/"));
            for (int i = 0; i < fileStatuses.length; i++) {
                FileStatus fileStatus = fileStatuses[i];
                Path srcPath = fileStatus.getPath();
                Path parent = srcPath.getParent();
                String parentPath = parent.toString() + "/";
                String fileName = srcPath.getName();
                Path sinkPath = new Path(parentPath + fileName + ".txt");
                if (fileName.startsWith("part-")) {
                    fs.rename(srcPath, sinkPath);
                }
            }
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
