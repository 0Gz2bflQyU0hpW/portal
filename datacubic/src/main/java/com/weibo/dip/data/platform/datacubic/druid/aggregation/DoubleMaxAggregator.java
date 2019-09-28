package com.weibo.dip.data.platform.datacubic.druid.aggregation;

/**
 * Created by yurun on 17/2/20.
 */
public class DoubleMaxAggregator extends Aggregator {

    private static final String DOUBLEMAX = "doubleMax";

    private String fieldName;

    public DoubleMaxAggregator(String name, String fieldName) {
        super(DOUBLEMAX, name);

        this.fieldName = fieldName;
    }

}
