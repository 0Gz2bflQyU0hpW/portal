package com.weibo.dip.data.platform.datacubic.business.summonagg.common;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.GsonUtil.GsonType;
import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import com.weibo.dip.data.platform.datacubic.business.util.AggConstants;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liyang28 on 2018/7/27.
 */
public class SummonApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(SummonApi.class);

  public SummonApi() {
  }

  /**
   * 发送post请求 .
   */
  private static String sendPost(String url, String params) {
    String result = "";
    try {
      result = HttpClientUtil.doPost(url, params);
    } catch (IOException e) {
      LOGGER.error("http client do post error {}", ExceptionUtils.getFullStackTrace(e));
    }
    return result;
  }

  /**
   * 获取 summon 接口的数据 .
   * @param business .
   * @param currentTimeMillis .
   * @return .
   */
  public static String getSummonRespResult(String business, long currentTimeMillis) {
    //summon接口，获取指标和纬度
    Map<String, Object> paramsMap = new HashMap<>();
    paramsMap.put(AggConstants.SUMMON_QUERY_TYPE, AggConstants.SUMMON_METADATA);
    paramsMap.put(AggConstants.SUMMON_DATA_SOURCE, business);
    paramsMap.put(AggConstants.TIMESTAMP, currentTimeMillis);
    paramsMap.put(AggConstants.SUMMON_ACCESS_KEY, AggConstants.ACCESS_KEY);
    String paramsJson = GsonUtil.toJson(paramsMap, GsonType.OBJECT_MAP_TYPE);
    return sendPost(AggConstants.SUMMON_URL, paramsJson);
  }

}
