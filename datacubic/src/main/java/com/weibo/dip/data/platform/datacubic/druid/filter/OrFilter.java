package com.weibo.dip.data.platform.datacubic.druid.filter;

/**
 * Created by yurun on 17/1/23.
 */
public class OrFilter extends Filter {

    private static final String OR = "or";

    private Filter[] fields;

    public OrFilter(Filter... fields) {
        super(OR);

        this.fields = fields;
    }

    public Filter[] getFields() {
        return fields;
    }

    public void setFields(Filter[] fields) {
        this.fields = fields;
    }

}
