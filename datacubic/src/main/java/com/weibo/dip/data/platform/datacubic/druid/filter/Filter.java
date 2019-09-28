package com.weibo.dip.data.platform.datacubic.druid.filter;

import com.weibo.dip.data.platform.commons.util.GsonUtil;

/**
 * Created by yurun on 17/1/22.
 */
public abstract class Filter {

    protected String type;

    public Filter(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return GsonUtil.toJson(this);
    }

    public String getFilter() {
        return toString();
    }

}
