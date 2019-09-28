package com.weibo.dip.data.platform.datacubic.druid.limit;

import com.weibo.dip.data.platform.commons.util.GsonUtil;

/**
 * Created by yurun on 17/3/29.
 */
public abstract class LimitSpec {

    @Override
    public String toString() {
        return GsonUtil.toJson(this);
    }

    public String getLimitSpec() {
        return toString();
    }

}
