package com.weibo.dip.data.platform.falcon.kafka.replica;

import com.weibo.dip.data.platform.commons.SinaWatchClient;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jianhong1 on 2018/9/3.
 */
public class UnderReplicatedMonitorMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnderReplicatedMonitorMain.class);
  private static final int THRESHOLD;

  static {
    Properties properties = new Properties();
    try {
      properties.load(new InputStreamReader(UnderReplicatedMonitorMain.class.getClassLoader().getResourceAsStream("under-replicated.properties")));
    } catch (IOException e) {
      throw new ExceptionInInitializerError("load properties file error: " + ExceptionUtils.getFullStackTrace(e));
    }
    THRESHOLD = Integer.parseInt(properties.getProperty("underReplicated"));
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
      Map<String, Integer> underReplicatedAllBroker = RestService.getUnderReplicated(startTime, endTime);
      Map<String, Integer> underReplicated = new HashMap<>();
      for (Map.Entry<String, Integer> entry: underReplicatedAllBroker.entrySet()) {
        if(entry.getValue() != 0){
          underReplicated.put(entry.getKey(), entry.getValue());
        }
      }
      if(underReplicated.size() == 0){
        LOGGER.info("total under replicated partitions:0, threshold:{}", THRESHOLD);
        return;
      }

      StringBuilder sb = new StringBuilder();
      int num = 0;
      for (Map.Entry entry: underReplicated.entrySet()) {
          num = num + (int)entry.getValue();
          String str = entry.getKey() + ":" + entry.getValue() + ",";
          sb.append(str);
      }
      sb.deleteCharAt(sb.length() - 1);
      LOGGER.warn("total under replicated partitions:{}, threshold:{}, [host:number] {}", num, THRESHOLD, sb.toString());

      //若under replica超过阈值，则触发报警
      if(num > THRESHOLD) {
        sinaWatchClient.alert("kafka", "UnderReplicatedPartitions", "total number:" + num + ", threshold:" + THRESHOLD, "[host:number] " + sb.toString(), "DIP_DATABUS");
      }
    } catch (IOException e){
      LOGGER.error("get UnderReplicatedPartitions error: {}", ExceptionUtils.getFullStackTrace(e));
      sinaWatchClient.alert("kafka", "UnderReplicatedPartitions", "get UnderReplicatedPartitions error:" + e.toString(), ExceptionUtils.getFullStackTrace(e), "DIP_DATABUS");
    }
  }
}