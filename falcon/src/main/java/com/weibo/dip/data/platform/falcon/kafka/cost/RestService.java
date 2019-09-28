package com.weibo.dip.data.platform.falcon.kafka.cost;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jianhong
 * @date 2018/4/18
 */
public class RestService {

  /**
   * http get method
   * @param url
   * @throws IOException
   */
  public void getMethod(String url) throws IOException {
    URL restURL = new URL(url);

    HttpURLConnection conn = (HttpURLConnection) restURL.openConnection();

    conn.setRequestMethod("GET"); // POST GET PUT DELETE
    conn.setRequestProperty("Accept", "application/json");

    BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    String line;
    while((line = br.readLine()) != null ){
      System.out.println(line);
    }

    br.close();
  }

  /**
   * http post method
   * @param url
   * @param query
   * @return
   * @throws IOException
   */
  public String postMethod(String url, String query) throws IOException {
    URL restURL = new URL(url);

    HttpURLConnection conn = (HttpURLConnection) restURL.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);

    PrintStream ps = new PrintStream(conn.getOutputStream());
    ps.print(query);
    ps.close();

    BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    String line, resultStr = "";
    while((line = br.readLine()) != null ){
      resultStr = resultStr + line + "\n";
    }

    br.close();

    return resultStr;
  }

  /**
   * 通过RestAPI获取active topic名称及其出口流量
   * @return
   * @throws IOException
   */
  public Map<String, BigDecimal> getTopicBytesOut(String yesterday) throws IOException {
    String query = "{\"queryType\":\"groupBy\",\"dataSource\":\"dip-kafka-monitor\",\"dimensions\":"
        + "[\"topic\"],\"granularity\":\"1d\",\"aggregations\":[{\"type\":\"max\",\"name\":\"BytesOutPerSec\",\"fieldName\":\"BytesOutPerSec\"}],"
        + "\"sort\":{\"BytesOutPerSec\":\"desc\"},\"size\":300,\"interval\":\"" + yesterday + " 00:00:00/" + yesterday + " 23:59:59\","
        + "\"timestamp\":2000000000000,\"accessKey\":\"gfpjJQ26D7B9632BEA7F\"}";

    String resultStr = postMethod("http://rest.summon.dip.weibo.com:8079/summon/rest/v2", query);

    Gson gson = new Gson();
    HashMap map = gson.fromJson(resultStr, HashMap.class);
    LinkedTreeMap treeMap = (LinkedTreeMap)map.get("data");
    ArrayList list = (ArrayList) treeMap.values().iterator().next();

    LinkedTreeMap treeMap1 = (LinkedTreeMap) list.get(0);
    ArrayList list1 = (ArrayList)treeMap1.get("topic");

//    System.out.println(list1.size());
    Map<String, BigDecimal> topicBytesOutMap = new HashMap<>();
    for (Object element: list1) {
      String topic = (String) ((LinkedTreeMap)element).get("key");
      BigDecimal value = BigDecimal.valueOf((Double) ((LinkedTreeMap) element).get("BytesOutPerSec"));

      //获取所有active topic信息，active topic为峰值流量不等于0的topic
      if(value.doubleValue() != 0){
        topicBytesOutMap.put(topic, value);
//        System.out.println(topic + " " + value.toPlainString());
      }
    }

    return topicBytesOutMap;
  }

  /**
   * 通过RestAPI获取产品线名称及其uuid
   * @return
   * @throws IOException
   */
  public Map<String, String> getProduceUuid() throws IOException {
    String resultStr = postMethod("http://rddss.intra.sina.com.cn/app/api/costunit/tree.php?level=4", "");
    HashMap<String, String> map = new HashMap();

    Gson gson = new Gson();
    ArrayList list = gson.fromJson(resultStr, ArrayList.class);

    for (Object element: list) {
      String uuid = (String) ((LinkedTreeMap)element).get("uuid");
      String productName = (String) ((LinkedTreeMap)element).get("name");

      map.put(productName, uuid);
    }
    return map;
  }

}
