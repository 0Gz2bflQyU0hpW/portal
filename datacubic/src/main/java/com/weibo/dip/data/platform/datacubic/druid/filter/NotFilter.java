package com.weibo.dip.data.platform.datacubic.druid.filter;

/**
 * Created by yurun on 17/1/23.
 */
public class NotFilter extends Filter {

    private static final String NOT = "not";

    private Filter field;

    public NotFilter(Filter field) {
        super(NOT);

        this.field = field;
    }

    public Filter getField() {
        return field;
    }

    public void setField(Filter field) {
        this.field = field;
    }

}
