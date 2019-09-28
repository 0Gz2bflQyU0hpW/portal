package com.weibo.dip.data.platform.datacubic.druid.aggregation;

/**
 * Created by yurun on 17/2/20.
 */
public class LongMinAggregator extends Aggregator {

    private static final String LONGMIN = "longMin";

    private String fieldName;

    public LongMinAggregator(String name, String fieldName) {
        super(LONGMIN, name);

        this.fieldName = fieldName;
    }

}
