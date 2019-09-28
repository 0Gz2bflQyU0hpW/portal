package com.weibo.dip.web.service.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.web.service.WarningCommonService;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;

import org.springframework.stereotype.Service;

/** Created by haisen on 2018/5/21. */
@Service
public class WarningCommonServiceImpl implements WarningCommonService {

  private static Gson gson = new GsonBuilder().create();

  private AnalyEsData esData = new AnalyEsData();

  private class AnalyEsData {
    private class Business {
      String business;
      Set<String> dimensions;
      Set<String> matries;

      public Business() {
        business = null;
        dimensions = new HashSet<>();
        matries = new HashSet<>();
      }

      public Business(String business) {
        this();
        this.business = business;
      }

      public String getBusiness() {
        return business;
      }

      public Set<String> getDimensions() {
        return dimensions;
      }

      public Set<String> getMatries() {
        return matries;
      }

      public void addDimensions(String dim) {
        dimensions.add(dim);
      }

      public void addMatries(String mat) {
        matries.add(mat);
      }

      @Override
      public String toString() {
        return business;
      }

      @Override
      public int hashCode() {
        return business.hashCode();
      }

      public boolean isExit() {
        return dimensions.size() > 0 & matries.size() > 0;
      }
    }

    private Map<String, Business> businessMap;

    public AnalyEsData() {
      businessMap = new HashMap<>();
      analyzedata();
    }

    private void analyzedata() {
      String response = getDataFromEs();
      businessMap.clear();
      Map<String, Object> responseJson =
          GsonUtil.fromJson(response, GsonUtil.GsonType.OBJECT_MAP_TYPE);

      Set<String> bussinessSet = ((Map) responseJson.get("data")).keySet();
      for (String business : bussinessSet) {
        // if (business.matches("^dip-alarm.+")) {
        Business mybusiness = new Business(business);
        ArrayList<Map<String, String>> valueArray =
            (ArrayList<Map<String, String>>) ((Map) responseJson.get("data")).get(business);
        for (int i = 0; i < valueArray.size(); i++) {
          String type = valueArray.get(i).get("type");
          String value = valueArray.get(i).get("field");
          if (type.equals("string")) {
            mybusiness.addDimensions(value);
          } else if (type.equals("int")
              || type.equals("long")
              || type.equals("float")
              || type.equals("double")
              || type.equals("byte")
              || type.equals("short")) {
            mybusiness.addMatries(value);
          }
        }
        businessMap.put(mybusiness.getBusiness(), mybusiness);
        // }
      }
    }

    public List<String> getAllBusiness() {
      analyzedata();
      List<String> business = new ArrayList<String>();
      for (Business business1 : businessMap.values()) {
        // if (business1.isExit()) {
        business.add(business1.getBusiness());
        // }
      }
      Collections.sort(business);
      return business;
    }

    public Set<String> getDimensionsBybusiness(String business) {
      if (business.equals("")) {
        return null;
      }
      TreeSet<String> dimensions = new TreeSet<>();
      Business business1 = businessMap.get(business);
      if (business1 != null /*& business1.isExit()*/) {
        dimensions.addAll(business1.getDimensions());
      }
      return dimensions;
    }

    public Set<String> getMetricsBybusiness(String business) {
      if (business.equals("")) {
        return null;
      }
      Set<String> metrics = new TreeSet<>();
      Business business1 = businessMap.get(business);
      if (business1 != null /*& business1.isExit()*/) {
        metrics.addAll(business1.getMatries());
      }
      return metrics;
    }

    /**
     * 从ES获取要解析的字符串.
     *
     * @return bussiness的字符串json格式的内容
     */
    private String getDataFromEs() {
      Long timestamp = new Date().getTime();
      Map<String, Object> request = new HashMap<>();
      request.put("queryType", "metadata");
      request.put("dataSource", "all");
      request.put("timestamp", timestamp);
      request.put("accessKey", "gfpjJQ26D7B9632BEA7F");

      HttpClient client = new HttpClient();

      /*client.getHttpConnectionManager().getParams().setConnectionTimeout(3000);
      client.getHttpConnectionManager().getParams().setSoTimeout(10000);*/

      PostMethod post = new PostMethod("http://rest.summon.dip.weibo.com:8079/summon/rest/v2");

      post.addRequestHeader("Content-Type", "application/json");

      String requestJson = gson.toJson(request);

      String response = null;

      try {
        post.setRequestEntity(new StringRequestEntity(requestJson, null, CharEncoding.UTF_8));
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }

      try {
        client.executeMethod(post);

        response =
            StringUtils.join(
                IOUtils.readLines(post.getResponseBodyAsStream(), CharEncoding.UTF_8), "\n");
      } catch (IOException e) {
        e.printStackTrace();
      }
      post.releaseConnection();
      return response;
    }
  }

  @Override
  public List<String> getAllBusiness() {
    return esData.getAllBusiness();
  }

  @Override
  public Set<String> getDimensionsBybusiness(String business) {
    return esData.getDimensionsBybusiness(business);
  }

  @Override
  public Set<String> getMetricsBybusiness(String business) {
    return esData.getMetricsBybusiness(business);
  }
}
