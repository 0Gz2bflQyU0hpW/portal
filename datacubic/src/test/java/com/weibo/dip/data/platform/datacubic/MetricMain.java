package com.weibo.dip.data.platform.datacubic;

import com.weibo.dip.data.platform.commons.metric.Metric;
import com.weibo.dip.data.platform.commons.metric.MetricStore;

/**
 * Created by yurun on 17/11/2.
 */
public class MetricMain {

    public static void main(String[] args) throws Exception {
        String business = "dip-test-data";

        long timestamp = System.currentTimeMillis();

        Metric<String> stringMetric = Metric.newEntity("key1", "str_value");
        Metric<Long> longMetric = Metric.newEntity("key2", 123L);
        Metric<Float> floatMetric = Metric.newEntity("key3", 1.23F);

        MetricStore.store(business, timestamp, stringMetric, longMetric, floatMetric);
    }

}
