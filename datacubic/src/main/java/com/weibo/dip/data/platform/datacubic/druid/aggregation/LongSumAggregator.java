package com.weibo.dip.data.platform.datacubic.druid.aggregation;

/**
 * Created by yurun on 17/2/20.
 */
public class LongSumAggregator extends Aggregator {

    private static final String LONGSUM = "longSum";

    private String fieldName;

    public LongSumAggregator(String name) {
        super(LONGSUM, name);

        this.fieldName = name;
    }

    public LongSumAggregator(String name, String fieldName) {
        super(LONGSUM, name);

        this.fieldName = fieldName;
    }

}
