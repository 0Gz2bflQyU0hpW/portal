package com.weibo.dip.data.platform.datacubic.druid.having;

/**
 * Created by yurun on 17/3/29.
 */
public class DimemsionHaving extends Having {

    private static final String DIM_SELECTOR = "dimSelector";

    private String dimension;

    private String value;

    public DimemsionHaving() {
        super(DIM_SELECTOR);
    }

    public DimemsionHaving(String dimension, String value) {
        this();

        this.dimension = dimension;
        this.value = value;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
