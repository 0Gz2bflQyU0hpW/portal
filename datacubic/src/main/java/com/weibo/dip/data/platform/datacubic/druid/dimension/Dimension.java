package com.weibo.dip.data.platform.datacubic.druid.dimension;

import com.weibo.dip.data.platform.commons.util.GsonUtil;

/**
 * Created by yurun on 17/2/14.
 */
public abstract class Dimension {

    private String type;

    public Dimension(String type) {
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

    public String getDimension() {
        return toString();
    }

}
