package com.weibo.dip.platform.yarn;

import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import java.io.IOException;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnResourceUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(YarnResourceUtil.class);

  private static final String URL_RM1 = "http://rm1.nyx.dip.weibo.com:8088/ws/v1/cluster/scheduler";
  private static final String URL_RM2 = "http://rm2.nyx.dip.weibo.com:8088/ws/v1/cluster/scheduler";

  /**
   * get queue catpaticy.
   *
   * @param queue yarn queue
   * @return capacity is available or not
   */
  public int getCapacity(String queue) {
    String response = "";
    try {
      response = HttpClientUtil.doGet(URL_RM2);
    } catch (IOException e) {
      LOGGER.error("check capacity of queue {}. {}", queue, ExceptionUtils.getFullStackTrace(e));
      try {
        response = HttpClientUtil.doGet(URL_RM1);
      } catch (IOException ioe) {
        LOGGER.error("check capacity of queue {}. {}", queue, ExceptionUtils.getFullStackTrace(e));
      }
    }

    JSONObject json = JSONObject.fromObject(response);
    if (!json.containsKey("scheduler")) {
      LOGGER.error("can not find JSONObject scheduler");
      return -1;
    }
    JSONObject scheduler = json.getJSONObject("scheduler");

    if (!scheduler.containsKey("schedulerInfo")) {
      LOGGER.error("can not find JSONObject schedulerInfo");
      return -1;
    }
    JSONObject schedulerInfo = scheduler.getJSONObject("schedulerInfo");

    if (!schedulerInfo.containsKey("queues")) {
      LOGGER.error("can not find JSONObject queues");
      return -1;
    }
    JSONObject queues = schedulerInfo.getJSONObject("queues");

    if (!queues.containsKey("queue")) {
      LOGGER.error("can not find JSONArray queues");
      return -1;
    }
    JSONArray queueArray = queues.getJSONArray("queue");

    if (queueArray.size() > 0) {
      for (int i = 0; i < queueArray.size(); i++) {
        JSONObject detail = queueArray.getJSONObject(i);

        if (!detail.containsKey("capacities")) {
          LOGGER.error("can not find JSONObject capacities");
          return -1;
        }
        JSONObject capacities = detail.getJSONObject("capacities");

        if (!capacities.containsKey("queueCapacitiesByPartition")) {
          LOGGER.error("can not find JSONObject queueCapacitiesByPartition");
          return -1;
        }
        JSONArray queueCapacitiesByPartition =
            capacities.getJSONArray("queueCapacitiesByPartition");

        if (queueCapacitiesByPartition.size() != 2) {
          LOGGER.error("the size of JSONArray 'queueCapacitiesByPartition' is not equals 2");
          return -1;
        }
        JSONObject used = queueCapacitiesByPartition.getJSONObject(1);

        if (used.containsKey("partitionName") && used.containsKey("absoluteUsedCapacity")) {
          String partitionName = used.getString("partitionName");
          double absoluteUsed = used.getDouble("absoluteUsedCapacity");
          // 队列名称相同、已用资源小于90%
          if (partitionName != null && partitionName.equals(queue) && absoluteUsed < 90d) {
            return 1;
          }
        }
      }
    } else {
      LOGGER.error("the size of JSONArray 'queue' is not bigger than 0");
    }
    return -1;
  }
}
