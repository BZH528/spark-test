package com.bzh.bigdata;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/14 10:48
 * @Description: 从网络上下载文件到本地系统
 */
public class DownloadFile {

    public static void main(String[] args) {
        try {
            downloadFile("https://gimg2.baidu.com/image_search/src=http%3A%2F%2Fcdn.duitang.com%2Fuploads%2Fitem%2F201202%2F18%2F20120218194349_ZHW5V.thumb.700_0.jpg&refer=http%3A%2F%2Fcdn.duitang.com&app=2002&size=f9999,10000&q=a80&n=0&g=0n&fmt=jpeg?sec=1623566089&t=32e4c4ea48d8f69cc45afe20cef1b64c",
                    "D:\\bzh\\guojiaorunwan\\jiqicat1234.jpeg");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    public static void downloadFile(String remoteFilePath, String localFilePath) throws MalformedURLException {
        URL urlFile = null;
        HttpURLConnection httpURLConnection = null;
        BufferedInputStream bis = null;
        BufferedOutputStream bos = null;
        File f = new File(localFilePath);

        urlFile = new URL(remoteFilePath);
        try {
            httpURLConnection = (HttpURLConnection) urlFile.openConnection();
//            httpURLConnection.connect();
            bis = new BufferedInputStream(httpURLConnection.getInputStream());
            bos = new BufferedOutputStream(new FileOutputStream(f));
            int len = 2048;
            byte[] b = new byte[len];
            while ((len = bis.read(b))!= -1) {
                bos.write(b,0,len);
            }
            bos.flush();
            bis.close();
            httpURLConnection.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bis.close();
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
