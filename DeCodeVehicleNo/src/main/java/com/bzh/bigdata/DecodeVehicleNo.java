package com.bzh.bigdata;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.net.URLDecoder;
import java.nio.Buffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: bizh
 * @Date: 2021/5/26 9:10
 * @Description: 通过post请求解密车牌号
 */
public class DecodeVehicleNo {

    public static void main(String[] args) throws IOException, InterruptedException {

        String url = "http://10.189.100.1:8080/rest1/vehicleNo/getsByMd5";

        //方法调取例子 application/x-www-form-urlencoded
       /* Map map = new HashMap<>();
        map.put("vehicleNo", "379E763944DF8C64F12E30DFC61B55B6");
        JSONObject jsonObjectResult = DecodeVehicleNo.postUrlencoded(url,map);*/

        ArrayList<String> lines = new ArrayList<>();
        File inputFile = new File(args[0]);
        InputStreamReader inputReader = new InputStreamReader(new FileInputStream(inputFile));
        BufferedReader br = new BufferedReader(inputReader);
        String str;
        while ((str = br.readLine()) != null) {
            lines.add(str.trim());
        }
        br.close();
        inputReader.close();

        Map map = new HashMap<>();
        ArrayList<String> outputResults = new ArrayList<>();

        for (String line : lines) {
            map.put("vehicleNo", line);
            JSONObject jsonObjectResult = DecodeVehicleNo.postUrlencoded(url,map);
            Thread.sleep(1000);
            String realVehicleNo = jsonObjectResult.getString("data");
            outputResults.add(realVehicleNo);
        }

        File outputFile = new File(args[1]);
        OutputStreamWriter outputWriter = new OutputStreamWriter(new FileOutputStream(outputFile));

        BufferedWriter bw = new BufferedWriter(outputWriter);
        for (String outResult : outputResults) {
            System.out.println("outputline:\t" + outResult);
            bw.write(outResult);
            bw.newLine();
            bw.flush();
        }
        bw.close();


    }



    public static JSONObject postUrlencoded(String url, Map<String, String> parms) throws IOException {
        HttpPost httpPost = new HttpPost(url);
        ArrayList<BasicNameValuePair> list = new ArrayList<>();
        parms.forEach((key, value) -> list.add(new BasicNameValuePair(key, value)));
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            if (Objects.nonNull(parms) && parms.size() >0) {
                httpPost.setEntity(new UrlEncodedFormEntity(list, "UTF-8"));
                RequestConfig requestConfig = RequestConfig.custom()
                        .setSocketTimeout(30000)//设置socket连接时长 毫秒为单位
                        .setConnectTimeout(30000)//设置连接时长
                        .setConnectionRequestTimeout(10000)//设置连接返回信息时长
                        .build();
                httpPost.setConfig(requestConfig);


            }
            InputStream content = httpPost.getEntity().getContent();
            InputStreamReader inputStreamReader = new InputStreamReader(content, "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String readLine = bufferedReader.readLine();
            String s = URLDecoder.decode(readLine, "UTF-8");
            System.out.println("readLine===================================" + readLine);
            System.out.println("s==========================================" + s);
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            JSONObject jsonObject = JSON.parseObject(EntityUtils.toString(entity, "UTF-8"));
            return jsonObject;
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (Objects.nonNull(httpClient)){
                try {
                    httpClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }


}
