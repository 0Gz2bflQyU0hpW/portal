package com.weibo.dip.data.platform.datacubic.druid.filter;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.druid.query.SortingOrder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yurun on 17/1/23.
 */
public class BoundFilter extends Filter {

    private static final String BOUND = "bound";

    private String dimension;

    private String lower;

    private String upper;

    private boolean lowerStrict;

    private boolean upperStrict;

    private SortingOrder ordering;

    public static BoundFilter lessThan(String dimension, String upper) {
        return new BoundFilter(dimension, null, upper, false, false, SortingOrder.lexicographic);
    }

    public static BoundFilter lessThan(String dimension, String upper, SortingOrder ordering) {
        return new BoundFilter(dimension, null, upper, false, false, ordering);
    }

    public static BoundFilter lessThan(String dimension, String upper, boolean upperStrict) {
        return new BoundFilter(dimension, null, upper, false, upperStrict, SortingOrder.lexicographic);
    }

    public static BoundFilter lessThan(String dimension, String upper, boolean upperStrict, SortingOrder ordering) {
        return new BoundFilter(dimension, null, upper, false, upperStrict, ordering);
    }

    public static BoundFilter greaterThan(String dimension, String lower) {
        return new BoundFilter(dimension, lower, null, false, false, SortingOrder.lexicographic);
    }

    public static BoundFilter greaterThan(String dimension, String lower, SortingOrder ordering) {
        return new BoundFilter(dimension, lower, null, false, false, ordering);
    }

    public static BoundFilter greaterThan(String dimension, String lower, boolean lowerStrict) {
        return new BoundFilter(dimension, lower, null, lowerStrict, false, SortingOrder.lexicographic);
    }

    public static BoundFilter greaterThan(String dimension, String lower, boolean lowerStrict, SortingOrder ordering) {
        return new BoundFilter(dimension, lower, null, lowerStrict, false, ordering);
    }

    public static BoundFilter buildBetween(String dimension, String lower, String upper) {
        return new BoundFilter(dimension, lower, upper, false, false, SortingOrder.lexicographic);
    }

    public static BoundFilter buildBetween(String dimension, String lower, String upper, SortingOrder ordering) {
        return new BoundFilter(dimension, lower, upper, false, false, ordering);
    }

    public static BoundFilter buildBetween(String dimension, String lower, String upper, boolean lowerStrict, boolean upperStrict) {
        return new BoundFilter(dimension, lower, upper, lowerStrict, upperStrict, SortingOrder.lexicographic);
    }

    public static BoundFilter buildBetween(String dimension, String lower, String upper, boolean lowerStrict, boolean upperStrict, SortingOrder ordering) {
        return new BoundFilter(dimension, lower, upper, lowerStrict, upperStrict, ordering);
    }

    public BoundFilter(String dimension, String lower, String upper, boolean lowerStrict, boolean upperStrict, SortingOrder ordering) {
        super(BOUND);

        this.dimension = dimension;
        this.lower = lower;
        this.upper = upper;
        this.lowerStrict = lowerStrict;
        this.upperStrict = upperStrict;
        this.ordering = ordering;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String getLower() {
        return lower;
    }

    public void setLower(String lower) {
        this.lower = lower;
    }

    public String getUpper() {
        return upper;
    }

    public void setUpper(String upper) {
        this.upper = upper;
    }

    public boolean isLowerStrict() {
        return lowerStrict;
    }

    public void setLowerStrict(boolean lowerStrict) {
        this.lowerStrict = lowerStrict;
    }

    public boolean isUpperStrict() {
        return upperStrict;
    }

    public void setUpperStrict(boolean upperStrict) {
        this.upperStrict = upperStrict;
    }

    public SortingOrder getOrdering() {
        return ordering;
    }

    public void setOrdering(SortingOrder ordering) {
        this.ordering = ordering;
    }

    @Override
    public String toString() {
        Map<String, Object> datas = new HashMap<>();

        datas.put("type", type);

        datas.put("dimension", dimension);

        if (lower != null) {
            datas.put("lower", lower);
            datas.put("lowerStrict", lowerStrict);
        }

        if (upper != null) {
            datas.put("upper", upper);
            datas.put("upperStrict", upperStrict);
        }

        datas.put("ordering", ordering);

        return GsonUtil.toJson(datas);
    }

}
