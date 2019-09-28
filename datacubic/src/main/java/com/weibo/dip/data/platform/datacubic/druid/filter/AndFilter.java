package com.weibo.dip.data.platform.datacubic.druid.filter;

/**
 * Created by yurun on 17/1/23.
 */
public class AndFilter extends Filter {

    private static final String AND = "and";

    private Filter[] fields;

    public AndFilter(Filter... fields) {
        super(AND);

        this.fields = fields;
    }

    public Filter[] getFields() {
        return fields;
    }

    public void setFields(Filter[] fields) {
        this.fields = fields;
    }

}
