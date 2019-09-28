package com.weibo.dip.data.platform.falcon.kafka.replica;

import com.weibo.dip.data.platform.commons.SinaWatchClient;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhiqiang32 on 2018/9/5.
 */
public class PreferredReplicaImbalanceMonitorMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(PreferredReplicaImbalanceMonitorMain.class);
  private static int THRESHOLD;
  static{
    Properties properties = new Properties();
    try {
      properties.load(new BufferedReader(new InputStreamReader(PreferredReplicaImbalanceMonitorMain.class.getClassLoader().getResourceAsStream("kafka-preferredreplicaimbalance.properties"))));
    } catch (IOException e) {
      LOGGER.error("load properties file error \n{}", ExceptionUtils.getFullStackTrace(e));
    }
    THRESHOLD = Integer.valueOf(properties.getProperty("imbalance"));
  }

  public static void main(String[] args) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //上一分钟的0秒
    String startTime = sdf.format((System.currentTimeMillis()-60000)/60000*60000);
    //当前时间
    String endTime = sdf.format(System.currentTimeMillis());
    LOGGER.info("start time:{}, end time:{}", startTime, endTime);

    SinaWatchClient sinaWatchClient = new SinaWatchClient();
    try{
      Map<String, Integer> preferredReplicaImbalanceAllBrokers = RestService.getPreferredReplicaImbalanceInfo(startTime, endTime);
      Map<String, Integer> preferredReplicaImbalance = new HashMap<>();
      for (Map.Entry<String, Integer> entry: preferredReplicaImbalanceAllBrokers.entrySet()) {
        if(entry.getValue() != 0){
          preferredReplicaImbalance.put(entry.getKey(), entry.getValue());
        }
      }
      if(preferredReplicaImbalance.size() == 0){
        LOGGER.info("total PreferredReplicaImbalanceCount:0");
        return;
      }

      StringBuilder sb = new StringBuilder();
      int num = 0;
      for (Map.Entry entry: preferredReplicaImbalance.entrySet()) {
          num = num + (int)entry.getValue();
          String str = entry.getKey() + ":" + entry.getValue() + ",";
          sb.append(str);
      }
      sb.deleteCharAt(sb.length() - 1);
      LOGGER.info("total PreferredReplicaImbalanceCount:"+ num);
      if (num>=THRESHOLD){
        LOGGER.warn("total PreferredReplicaImbalanceCount:{}, [kafka-controller:count] {}", num, sb.toString());
        sinaWatchClient.alert("kafka", "PreferredReplicaImbalanceCount", "total :" + num, "[kafka-controller:count] " + sb.toString(), "DIP_DATABUS");
      }
    } catch (IOException e){
      LOGGER.error("get PreferredReplicaImbalanceCount error: {}", ExceptionUtils.getFullStackTrace(e));
      sinaWatchClient.alert("kafka", "PreferredReplicaImbalanceCount", "get PreferredReplicaImbalanceCount error:" + e.toString(), ExceptionUtils.getFullStackTrace(e), "DIP_DATABUS");
    }
  }
}