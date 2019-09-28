package com.weibo.dip.data.platform.datacubic.druid.query.builder;

import com.weibo.dip.data.platform.datacubic.druid.query.GroupBy;
import com.weibo.dip.data.platform.datacubic.druid.query.Timeseries;

/**
 * Created by yurun on 17/1/24.
 */
public class QueryBuilderFactory {

    public static TimeseriesBuilder createTimeseriesBuilder() {
        return new TimeseriesBuilder(new Timeseries());
    }

    public static GroupByBuilder createGroupByBuilder() {
        return new GroupByBuilder(new GroupBy());
    }

}
