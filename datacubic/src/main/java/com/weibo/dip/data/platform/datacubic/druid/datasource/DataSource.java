package com.weibo.dip.data.platform.datacubic.druid.datasource;

import com.weibo.dip.data.platform.commons.util.GsonUtil;

/**
 * Created by yurun on 17/2/14.
 */
public abstract class DataSource {

    private String type;

    public DataSource(String type) {
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

    public String getDatasource() {
        return toString();
    }

}
