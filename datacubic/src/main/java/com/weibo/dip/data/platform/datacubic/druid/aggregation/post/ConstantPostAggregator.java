package com.weibo.dip.data.platform.datacubic.druid.aggregation.post;

/**
 * Created by yurun on 17/3/29.
 */
public class ConstantPostAggregator extends PostAggregator {

    private static final String CONSTANT = "constant";

    private String name;

    private Number value;

    public ConstantPostAggregator(String name, Number value) {
        super(CONSTANT, name);

        this.name = name;
        this.value = value;
    }

    public ConstantPostAggregator(String name, int value) {
        this(name, (Number) value);
    }

    public ConstantPostAggregator(String name, long value) {
        this(name, (Number) value);
    }

    public ConstantPostAggregator(String name, float value) {
        this(name, (Number) value);
    }

    public ConstantPostAggregator(String name, double value) {
        this(name, (Number) value);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public Number getValue() {
        return value;
    }

    public void setValue(Number value) {
        this.value = value;
    }

}
