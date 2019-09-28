package com.weibo.dip.data.platform.datacubic.druid.having;

/**
 * Created by yurun on 17/3/29.
 */
public class MetricHaving extends Having {

    public static final String EQUAL_TO = "equalTo";
    public static final String GREATER_THAN = "greaterThan";
    public static final String LESS_THAN = "lessThan";

    private String aggregation;

    private Number value;

    public MetricHaving(String type, String aggregation, Number value) {
        super(type);

        this.aggregation = aggregation;
        this.value = value;
    }

    public MetricHaving(String type, String aggregation, int value) {
        this(type, aggregation, (Number) value);
    }

    public MetricHaving(String type, String aggregation, long value) {
        this(type, aggregation, (Number) value);
    }

    public MetricHaving(String type, String aggregation, float value) {
        this(type, aggregation, (Number) value);
    }

    public MetricHaving(String type, String aggregation, double value) {
        this(type, aggregation, (Number) value);
    }

    public String getAggregation() {
        return aggregation;
    }

    public void setAggregation(String aggregation) {
        this.aggregation = aggregation;
    }

    public Number getValue() {
        return value;
    }

    public void setValue(Number value) {
        this.value = value;
    }

}
