package com.weibo.dip.data.platform.datacubic.druid.limit;

import com.weibo.dip.data.platform.datacubic.druid.query.SortingOrder;

/**
 * Created by yurun on 17/3/29.
 */
public class OrderByColumnSpec extends LimitSpec {

    public static final String ASCENDING = "ascending";
    public static final String DESCENDING = "descending";

    private String dimension;

    private String direction = ASCENDING;

    private SortingOrder dimensionOrder = SortingOrder.lexicographic;

    public OrderByColumnSpec() {
    }

    public OrderByColumnSpec(String dimension) {
        this.dimension = dimension;
    }

    public OrderByColumnSpec(String dimension, String direction, SortingOrder dimensionOrder) {
        this.dimension = dimension;
        this.direction = direction;
        this.dimensionOrder = dimensionOrder;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public SortingOrder getDimensionOrder() {
        return dimensionOrder;
    }

    public void setDimensionOrder(SortingOrder dimensionOrder) {
        this.dimensionOrder = dimensionOrder;
    }

}
