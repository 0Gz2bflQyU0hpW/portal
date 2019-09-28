package com.weibo.dip.data.platform.datacubic.druid.aggregation.post;

import com.weibo.dip.data.platform.datacubic.druid.aggregation.Aggregator;

/**
 * Created by yurun on 17/3/29.
 */
public class ArithmeticPostAggregator extends PostAggregator {

    private static final String ARITHMETIC = "arithmetic";

    public static final String PLUS = "+";
    public static final String MINUS = "-";
    public static final String MULTIPLICATION = "*";
    public static final String DIVISION = "/";
    public static final String QUOTIENT = "quotient";

    public static final String NUMERIC_FIRST = "numericFirst";

    private String fn;

    private Aggregator[] fields;

    private String ordering = NUMERIC_FIRST;

    public ArithmeticPostAggregator(String name, String fn, PostAggregator... fields) {
        super(ARITHMETIC, name);

        this.fn = fn;
        this.fields = fields;
    }

    public String getFn() {
        return fn;
    }

    public void setFn(String fn) {
        this.fn = fn;
    }

    public Aggregator[] getFields() {
        return fields;
    }

    public void setFields(Aggregator[] fields) {
        this.fields = fields;
    }

}
