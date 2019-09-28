package com.weibo.dip.data.platform.datacubic.druid.limit;

/**
 * Created by yurun on 17/3/29.
 */
public class DefaultLimitSpec extends LimitSpec {

    private String type = "default";

    private int limit;

    private OrderByColumnSpec[] columns;

    public DefaultLimitSpec() {
    }

    public DefaultLimitSpec(int limit) {
        this.limit = limit;
    }

    public DefaultLimitSpec(int limit, OrderByColumnSpec... columns) {
        this.limit = limit;
        this.columns = columns;
    }

    public String getType() {
        return type;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public OrderByColumnSpec[] getColumns() {
        return columns;
    }

    public void setColumns(OrderByColumnSpec[] columns) {
        this.columns = columns;
    }

}
