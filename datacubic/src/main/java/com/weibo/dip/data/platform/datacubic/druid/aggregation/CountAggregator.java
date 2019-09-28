package com.weibo.dip.data.platform.datacubic.druid.aggregation;

/**
 * Created by yurun on 17/2/20.
 */
public class CountAggregator extends Aggregator {

    public static final String COUNT = "count";

    public CountAggregator() {
        this(COUNT);
    }

    public CountAggregator(String name) {
        super(COUNT, name);
    }

}
