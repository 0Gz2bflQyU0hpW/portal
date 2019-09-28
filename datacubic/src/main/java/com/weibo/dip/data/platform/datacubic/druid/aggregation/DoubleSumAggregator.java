package com.weibo.dip.data.platform.datacubic.druid.aggregation;

/**
 * Created by yurun on 17/2/20.
 */
public class DoubleSumAggregator extends Aggregator {

    private static final String DOUBLESUM = "doubleSum";

    private String fieldName;

    public DoubleSumAggregator(String name, String fieldName) {
        super(DOUBLESUM, name);

        this.fieldName = fieldName;
    }

}
