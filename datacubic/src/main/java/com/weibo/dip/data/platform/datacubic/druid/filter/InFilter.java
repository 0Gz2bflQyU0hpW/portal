package com.weibo.dip.data.platform.datacubic.druid.filter;

/**
 * Created by yurun on 17/1/23.
 */
public class InFilter extends Filter {

    private static final String IN = "in";

    private String dimension;

    private String[] values;

    public InFilter(String dimension, String[] values) {
        super(IN);

        this.dimension = dimension;
        this.values = values;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String[] getValues() {
        return values;
    }

    public void setValues(String[] values) {
        this.values = values;
    }

}
