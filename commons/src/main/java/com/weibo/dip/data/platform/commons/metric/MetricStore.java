package com.weibo.dip.data.platform.commons.metric;

import com.google.common.base.Preconditions;
import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

/** Created by yurun on 17/11/2. */
public class MetricStore {
  private static final String SUMMON_REST = "http://api.dip.weibo.com:9083/summon/write";

  private static final String SUMMON_BUSINESS = "business";
  private static final String SUMMON_TIMESTAMP = "timestamp";
  private static final String SUMMON_PARAMETERS = "parameters";

  private static final String SUMMON_FORMAT = "%s|%s|%s;";

  private MetricStore() {}

  /**
   * Store metrics.
   *
   * @param business business name
   * @param timestamp timestamp in millisecond
   * @param metrics one or more metrics
   * @throws Exception if store failure
   */
  public static void store(String business, long timestamp, Metric<?>... metrics) throws Exception {
    Preconditions.checkState(StringUtils.isNotEmpty(business), "business must be specified");
    Preconditions.checkState(ArrayUtils.isNotEmpty(metrics), "metrics must be specified");

    Map<String, String> request = new HashMap<>();

    request.put(SUMMON_BUSINESS, business);
    request.put(SUMMON_TIMESTAMP, String.valueOf(timestamp));

    StringBuilder parameters = new StringBuilder();

    for (Metric<?> metric : metrics) {
      Preconditions.checkState(Objects.nonNull(metric), "metrics has null entity");

      parameters.append(
          String.format(
              SUMMON_FORMAT,
              metric.getName(),
              String.valueOf(metric.getValue()),
              metric.getValue().getClass().getSimpleName().toLowerCase()));
    }

    request.put(SUMMON_PARAMETERS, parameters.toString());

    HttpClientUtil.doPost(SUMMON_REST, request);
  }
}
