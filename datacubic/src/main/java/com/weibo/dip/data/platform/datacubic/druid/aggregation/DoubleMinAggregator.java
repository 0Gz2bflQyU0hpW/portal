package com.weibo.dip.data.platform.datacubic.druid.aggregation;

/**
 * Created by yurun on 17/2/20.
 */
public class DoubleMinAggregator extends Aggregator {

    private static final String DOUBLEMIN = "doubleMin";

    private String fieldName;

    public DoubleMinAggregator(String name, String fieldName) {
        super(DOUBLEMIN, name);

        this.fieldName = fieldName;
    }

}
