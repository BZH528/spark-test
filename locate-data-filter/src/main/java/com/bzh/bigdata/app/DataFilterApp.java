package com.bzh.bigdata.app;

import com.bzh.bigdata.util.LocateUtils;
import com.bzh.bigdata.util.MyPropertiesUtil;
import com.bzh.bigdata.util.MysqlUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/19 15:12
 * @Description:
 */
public class DataFilterApp {


    // WIndows下防止idea本地执行报错
    /*static {
        try {
            System.load("D:\\bzh\\guojiaorunwan\\tools\\winutils-master\\hadoop-3.0.0\\bin\\hadoop.dll");//建议采用绝对地址，bin目录下的hadoop.dll文件路径
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }*/

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 2) {
            System.err.println("Usage: <inputPath>... <outputPath>");
            return;
        }


        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://runone01:8020");

        Job job = Job.getInstance(conf);
        job.setJarByClass(DataFilterApp.class);

        // 指定Mapper和输出类型
        job.setMapperClass(DataFilterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);//不需要reduce任务

        // 指定数据的输入路径和输出路径
        for (int i = 0; i < args.length - 1; i++) {
            Path inputPath = new Path(args[i]);
            FileInputFormat.addInputPath(job, inputPath);
        }

        Path outputPath = new Path(args[args.length - 1]);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileOutputFormat.setOutputPath(job,outputPath);

        // 提交任务
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion?0:1);

    }

    private static class DataFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        public Text outputKey = new Text();
        public NullWritable outputValue = NullWritable.get();

        List<String> logintitudes = new ArrayList<>();
        List<String> latitudes = new ArrayList<>();
        Properties properties = MyPropertiesUtil.load("config.properties");
        String sqlStr = properties.getProperty("data.highway.sql.str");
        Double max_distance = Double.valueOf(properties.getProperty("data.highway.compute.distance"));

        private Logger logger;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            // 获取mysql库表中经纬度的集合
            MysqlUtils.getHighWayInfoFromMysql(sqlStr, logintitudes, latitudes);
            this.logger = LoggerFactory.getLogger(DataFilterMapper.class);

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\u0001"); //hive默认分隔符
            String longtitude = fields[2];
            String latitude = fields[3];
            if (isInHighWay(longtitude, latitude, logintitudes, latitudes, max_distance)) {
                // 如果当前坐标在这个高速路上则写出到hdfs
                outputKey.set(line);
                context.write(outputKey, outputValue);
                logger.info("in highway msg : " + line);
            }

        }
    }

    //判断是否是在高速路上
    private static boolean isInHighWay(String target_longtitude, String target_latitude, List<String> longitudes, List<String> latitudes, double max_distance) {
        return LocateUtils.isInHighWayAccordLocate(target_longtitude, target_latitude, longitudes, latitudes);
    }
}
