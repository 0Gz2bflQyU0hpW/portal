package com.weibo.dip.web.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by haisen on 2018/4/26.
 */
public class AnalyzeJson {

  //only use to analyzeing the JSON data
  private Map<String, String> map;

  public AnalyzeJson(String data) {
    map = new HashMap<String, String>();
    analyzeString(data);
  }

  private void analyzeString(String data) {
    JSONObject jsonObject = new JSONObject(data);
    map.put("start", String.valueOf(jsonObject.getInt("start")));
    map.put("length", String.valueOf(jsonObject.getInt("length")));
    JSONArray jsonArray = jsonObject.getJSONArray("order");
    map.put("column", String.valueOf(jsonArray.getJSONObject(0).getInt("column")));
    map.put("dir", jsonArray.getJSONObject(0).getString("dir"));
    map.put("condition", jsonObject.optString("condition", ""));
    map.put("starttime", jsonObject.optString("starttime", ""));
    map.put("endtime", jsonObject.optString("endtime", ""));
    map.put("keyword", jsonObject.optString("keyword", ""));
  }

  public String getCondition() {
    return map.get("condition");
  }

  public String getStarttime() {
    return map.get("starttime");
  }

  public String getEndtime() {
    return map.get("endtime");
  }

  public String getKeyword() {
    return map.get("keyword");
  }

  public int getColumn() {
    return Integer.parseInt(map.get("column"));
  }

  public String getDir() {
    return map.get("dir");
  }

  /**
   * json变为String.
   *
   * @param list 查询结果
   * @return 返回传给客户端的String
   */
  public String getResponseString(List list) {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("iTotalRecords", list.size());
    jsonObject.put("iTotalDisplayRecords", list.size());
    int end = Integer.parseInt(map.get("start")) + Integer.parseInt(map.get("length"));
    end = (end > list.size()) ? list.size() : end;
    jsonObject.put("aaData", list.subList(Integer.parseInt(map.get("start")), end));
    return jsonObject.toString();
  }
}
