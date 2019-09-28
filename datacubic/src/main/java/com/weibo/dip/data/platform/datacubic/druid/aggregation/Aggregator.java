package com.weibo.dip.data.platform.datacubic.druid.aggregation;

import com.weibo.dip.data.platform.commons.util.GsonUtil;

/**
 * Created by yurun on 17/2/20.
 */
public abstract class Aggregator {

    protected String type;

    protected String name;

    public Aggregator(String type, String name) {
        this.type = type;
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return GsonUtil.toJson(this);
    }

    public String getAggregation() {
        return toString();
    }

}
