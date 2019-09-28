package com.weibo.dip.data.platform.datacubic.druid.aggregation;

/**
 * Created by yurun on 17/2/20.
 */
public class LongMaxAggregator extends Aggregator {

    private static final String LONGMAX = "longMax";

    private String fieldName;

    public LongMaxAggregator(String name) {
        super(LONGMAX, name);

        this.fieldName = name;
    }

    public LongMaxAggregator(String name, String fieldName) {
        super(LONGMAX, name);

        this.fieldName = fieldName;
    }

}
