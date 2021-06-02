package com.bzh.bigdata;

import org.apache.avro.util.ByteBufferOutputStream;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/14 14:25
 * @Description:
 */
public class Test {

    public static void main(String[] args) throws IOException {
        f("https://gimg2.baidu.com/image_search/src=http%3A%2F%2Fcdn.duitang.com%2Fuploads%2Fitem%2F201202%2F18%2F20120218194349_ZHW5V.thumb.700_0.jpg&refer=http%3A%2F%2Fcdn.duitang.com&app=2002&size=f9999,10000&q=a80&n=0&g=0n&fmt=jpeg?sec=1623566089&t=32e4c4ea48d8f69cc45afe20cef1b64c",
                "D:\\bzh\\guojiaorunwan\\jiqicat.jpeg");
    }

    public static void f (String source_url, String sink_url) throws IOException {
        URL url =  new URL(source_url);
        HttpURLConnection httpURLConnection = (HttpURLConnection)url.openConnection();
        InputStream inputStream = httpURLConnection.getInputStream();

        BufferedInputStream bis = new BufferedInputStream(inputStream);
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(new File(sink_url)));

        int len = 2048;
        byte[] buff = new byte[len];

        ;
        while ((len= bis.read(buff, 0, len)) != -1) {
            bos.write(buff,0,len);
        }

        bos.flush();
        bos.close();
        bis.close();
        httpURLConnection.disconnect();

    }
}
