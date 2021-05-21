package com.bzh.bigdata.util;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class MergeFile {

    private FileStatus[] fileStatuses;
    private FileSystem fileSystem;

    private long fileTotalSize;
    private long MAX_BLOCK_SIZE;

    private String source_url = null;
    private String sink_url = null;
    private String baseurl = "hdfs://runone01:8020";

    private String file_suffix;
    private long block_size;
    private String out_suffex;
    private boolean need_delete;

    private Logger logger;

    public MergeFile(Properties proc, String source_url, String sink_url) {
        this.logger = LoggerFactory.getLogger(MergeFile.class);
        this.source_url = source_url;
        this.sink_url = sink_url;
        this.file_suffix = proc.getProperty("hdfs.file.name.suffix", ".txt");
        String block_size_proc = proc.getProperty("merge.output.file.maxsize", "128");
        this.block_size = Long.valueOf(block_size_proc);
        this.MAX_BLOCK_SIZE = this.block_size * 1000 * 1000;
        this.out_suffex = proc.getProperty("merge.output.file.name.suffix", ".merge");
        String need_delete_proc = proc.getProperty("merge.output.delete_origin_file", "false");
        this.need_delete = Boolean.valueOf(need_delete_proc);
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        URI uri = null;
        try {
            uri = new URI(baseurl);
            fileSystem = FileSystem.get(uri, conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        fileTotalSize = 0;
    }

    public FileStatus[] getFileList() throws IOException {
        this.fileStatuses = fileSystem.listStatus(new Path(this.source_url));
        return this.fileStatuses;
    }

    public void mergeFile() {
        if (this.fileStatuses == null || this.fileStatuses.length == 0) {
            return;
        }
        FSDataOutputStream out = null;
        long start_time = System.currentTimeMillis();
        int count = 0;
        for (int i = 0; i < this.fileStatuses.length; i++) {
            FileStatus fileStatus = this.fileStatuses[i];
            Path path = fileStatus.getPath();
            String name = path.getName();
            //判断是否是以.txt结尾
            int index = name.indexOf(".txt");
            boolean flag = false;
            if (index == name.length() - 4) {
                flag = true;
            }
            try {
                if (flag) {
                    InputStream open = fileSystem.open(path);
                    if (needNextBlockFile(fileStatus) || count == 0) {
                        if (out != null) {
                            out.flush();
                            out.close();
                        }
                        String fileName = this.getFileName();
                        out = fileSystem.create(new Path(sink_url + "/" + fileName));
                        fileTotalSize = 0;
                    }
                    IOUtils.copy(open, out);
                    out.flush();
                    open.close();
                    fileTotalSize += fileStatus.getLen();
                    if (this.need_delete) {
                        fileSystem.delete(path, false);
                    }
                    count++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.logger.info(name + "\t" + flag + "\t" + count);
        }
        if (out != null) {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long end_time = System.currentTimeMillis();
        long total = (end_time - start_time) / 1000;
        this.logger.info("总共花费时间为：" + total + "秒");
        this.close();
    }

    private boolean needNextBlockFile(FileStatus fileStatus) {
        long len = fileStatus.getLen();
        if (this.fileTotalSize + len >= MAX_BLOCK_SIZE) {
            return true;
        }
        return false;
    }

    private String getFileName() {
        Date date = new Date();
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(date);
        Random random = new Random();
        return time + (random.nextInt(900) + 100) + ".merge";
    }

    private void close() {
        if (this.fileSystem != null) {
            try {
                this.fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void run() {
        this.logger.info("开始获取文件夹下所有的文件信息...");
        try {
            this.getFileList();
            this.logger.info("文件信息已经获取到！");
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.logger.info("开始合并文件...");
        this.mergeFile();
        this.logger.info("合并完毕!");
    }

    /**
     * # 主要是针对定位数据做合并，注意：该过程不允许中断，否则会丢失数据。如果后期跟踪、告警数据也有很多文件的话，类似方法合并。
     #  参数1： conf/merge.properties 合并配置文件的位置。
     #  参数2： /data/hive_ods/${code}/${code}_locate/in_date=${dt} 要合并的文件目录，这里合并配置文件里面带有指定后缀的文件，例如：后缀为.txt文件。
     #  参数3： 合并的结果文件输出位置。这里省略了，缺省则默认和参数2的相同。
        java -jar mergesmallfile-1.0.jar conf/merge_shenshanxi.properties /mr/output/shenshanxi/filter-2021-05
     *
     * merge_shenshanxi.properties:
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            return;
        }
        String conf_path = args[0];
        Properties conf = new Properties();
        try {
            FileInputStream fileInputStream = new FileInputStream(new File(conf_path));
            conf.load(fileInputStream);
            fileInputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String source_url = args[1];
//        String source_url = "/data/hive_ods/4401/4401_locate/in_date=2020-06-16";
        String sink_url = source_url;
        if (args.length >= 3) {
            sink_url = args[2];
        }

        MergeFile mergeFile = new MergeFile(conf, source_url, sink_url);
        mergeFile.run();
    }

}

