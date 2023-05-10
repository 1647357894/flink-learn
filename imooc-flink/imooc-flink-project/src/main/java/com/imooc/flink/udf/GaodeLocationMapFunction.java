package com.imooc.flink.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.imooc.flink.domain.Access;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * @author wangjie
 * @date 2021/11/25 下午10:38
 */
public class GaodeLocationMapFunction extends RichMapFunction<Access,Access> {
CloseableHttpClient client;
    @Override
    public void open(Configuration parameters) throws Exception {
        client = HttpClients.createDefault();
    }

    @Override
    public void close() throws Exception {
        if (client!=null) {
            client.close();
        }
    }

    @Override
    public Access map(Access value) throws Exception {




        String ip =  value.ip;

        String province = "-";
        String city = "-";

        String key = "0e59dff213571da1d35e15a3c77ad4ff";
        String url = "https://restapi.amap.com/v3/ip?ip="+ip+"&output=json&key="+ key;

        CloseableHttpClient httpClient = HttpClients.createDefault();

        CloseableHttpResponse response = null;

        try {
            HttpGet httpGet = new HttpGet(url);
            response = httpClient.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if(statusCode == 200) {
                HttpEntity entity = response.getEntity();
                String result = EntityUtils.toString(entity, "UTF-8");


                JSONObject jsonObject = JSON.parseObject(result);
                province = jsonObject.getString("province");
                city = jsonObject.getString("city");

                System.out.println(province + "\t" + city);
                value.province = province;
                value.city = city;

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(null != response) try {
                response.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        return value;
    }
}
