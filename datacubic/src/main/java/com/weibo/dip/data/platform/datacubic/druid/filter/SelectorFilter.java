package com.weibo.dip.data.platform.datacubic.druid.filter;

/**
 * Created by yurun on 17/1/22.
 */
public class SelectorFilter extends Filter {

    private static final String SELECTOR = "selector";

    private String dimension;

    private String value;

    public SelectorFilter() {
        super(SELECTOR);
    }

    public SelectorFilter(String dimension, String value) {
        super(SELECTOR);

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
