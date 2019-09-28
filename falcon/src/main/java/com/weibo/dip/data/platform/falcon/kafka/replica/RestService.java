package com.weibo.dip.data.platform.falcon.kafka.replica;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jianhong1 on 2018/8/13.
 */
public class RestService {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestService.class);

  //http post method
  public static String postMethod(String url, String query) throws IOException {
    URL restURL = new URL(url);

    HttpURLConnection conn = (HttpURLConnection) restURL.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);

    PrintStream ps = new PrintStream(conn.getOutputStream());
    ps.print(query);
    ps.close();

    BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    StringBuilder sb = new StringBuilder();
    String line;
    while((line = br.readLine()) != null ){
      sb.append(line);
    }
    br.close();

    return sb.toString();
  }

  //获取所有broker的UnderReplicatedPartitions数目，格式<topic, UnderReplicatedPartitions>
  public static Map<String, Integer> getUnderReplicated(String startTime, String endTime) throws IOException {
    String query =
        "{\"queryType\":\"groupBy\",\"dataSource\":\"dip-kafka-broker-monitor\",\"dimensions\":"
            + "[\"broker\"],\"granularity\":\"1m\",\"filter\":{\"type\":\"and\",\"fields\":"
            + "[{\"cluster\":\"k1001\",\"type\":\"equal\"}]},\"aggregations\":"
            + "[{\"type\":\"max\",\"name\":\"UnderReplicatedPartitions\",\"fieldName\":\"UnderReplicatedPartitions\"}],"
            + "\"size\":1000,\"interval\":\"" + startTime + "/" + endTime + "\","
            + "\"timestamp\":2000000000000,\"accessKey\":\"gfpjJQ26D7B9632BEA7F\"}";

    String resultStr = postMethod("http://rest.summon.dip.weibo.com:8079/summon/rest/v2", query);
    if(StringUtils.isEmpty(resultStr)){
      throw new IOException("result returned from rest is empty");
    }

    HashMap map = new Gson().fromJson(resultStr, HashMap.class);
    LinkedTreeMap treeMap = (LinkedTreeMap)map.get("data");
    ArrayList list = (ArrayList)treeMap.get("1m");
    LinkedTreeMap treeMap1 = (LinkedTreeMap) list.get(0);

    ArrayList brokerList = (ArrayList)treeMap1.get("broker");
    LOGGER.info("broker number: {}", brokerList.size());

    Map<String, Integer> underReplicatedMap = new HashMap<>();
    for (Object element : brokerList) {
      String broker = (String) ((LinkedTreeMap)element).get("key");
      Double underReplicated = (Double)((LinkedTreeMap)element).get("UnderReplicatedPartitions");
      underReplicatedMap.put(broker, underReplicated.intValue());
    }

    return underReplicatedMap;
  }


  /**
  * @Description:  获取kafka controller的PreferredReplicaImbalance的信息
  * @Date: 下午4:07 2018/9/5
  * @Param: [startTime, endTime]
  * @return: java.util.Map<java.lang.String,java.lang.Integer>
  */
  public static Map<String, Integer> getPreferredReplicaImbalanceInfo(String startTime, String endTime) throws IOException {
    String query =
            "{\"queryType\":\"groupBy\",\"dataSource\":\"dip-kafka-broker-monitor\",\"dimensions\":"
                    + "[\"broker\"],\"granularity\":\"1m\",\"filter\":{\"type\":\"and\",\"fields\":"
                    + "[{\"cluster\":\"k1001\",\"type\":\"equal\"}]},\"aggregations\":"
                    + "[{\"type\":\"max\",\"name\":\"PreferredReplicaImbalanceCount\",\"fieldName\":\"PreferredReplicaImbalanceCount\"}],"
                    + "\"size\":1000,\"interval\":\"" + startTime + "/" + endTime + "\","
                    + "\"timestamp\":2000000000000,\"accessKey\":\"gfpjJQ26D7B9632BEA7F\"}";

    String resultStr = postMethod("http://rest.summon.dip.weibo.com:8079/summon/rest/v2", query);
    if(StringUtils.isEmpty(resultStr)){
      throw new IOException("result returned from rest is empty");
    }

    HashMap map = new Gson().fromJson(resultStr, HashMap.class);
    LinkedTreeMap treeMap = (LinkedTreeMap)map.get("data");
    ArrayList list = (ArrayList)treeMap.get("1m");
    LinkedTreeMap treeMap1 = (LinkedTreeMap) list.get(0);

    ArrayList brokerList = (ArrayList)treeMap1.get("broker");
    LOGGER.info("broker number: {}", brokerList.size());

    Map<String, Integer> underReplicatedMap = new HashMap<>();
    for (Object element : brokerList) {
      String broker = (String) ((LinkedTreeMap)element).get("key");
      Double underReplicated = (Double)((LinkedTreeMap)element).get("PreferredReplicaImbalanceCount");
      underReplicatedMap.put(broker, underReplicated.intValue());
    }

    return underReplicatedMap;
  }

  public static void main(String[] args) throws IOException {
    String startTime = "2018-09-02 13:58:00";
    String endTime = "2018-09-02 13:59:00";

    Map<String, Integer> underReplicatedMap = getUnderReplicated(startTime, endTime);
    for (Map.Entry<String, Integer> entry: underReplicatedMap.entrySet()) {
      System.out.println(entry.getKey() + ":" + entry.getValue());
    }
  }
}
