package com.weibo.dip.data.platform.commons.metric;

/** @author yurun */
public class MetricStoreTester {
  public static void main(String[] args) throws Exception {
    String business = "business_test";

    long timestamp = System.currentTimeMillis();

    Metric<String> stringMetric = Metric.newEntity("key1", "str_value");
    Metric<Long> longMetric = Metric.newEntity("key2", 123L);
    Metric<Float> floatMetric = Metric.newEntity("key3", 1.23F);

    MetricStore.store(business, timestamp, stringMetric, longMetric, floatMetric);
  }
}
