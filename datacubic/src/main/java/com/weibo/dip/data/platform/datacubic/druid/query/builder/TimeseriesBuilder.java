package com.weibo.dip.data.platform.datacubic.druid.query.builder;

import com.weibo.dip.data.platform.datacubic.druid.aggregation.Aggregator;
import com.weibo.dip.data.platform.datacubic.druid.aggregation.post.PostAggregator;
import com.weibo.dip.data.platform.datacubic.druid.filter.Filter;
import com.weibo.dip.data.platform.datacubic.druid.query.Granularity;
import com.weibo.dip.data.platform.datacubic.druid.query.Interval;
import com.weibo.dip.data.platform.datacubic.druid.query.Intervals;
import com.weibo.dip.data.platform.datacubic.druid.query.Timeseries;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created by yurun on 17/1/24.
 */
public class TimeseriesBuilder extends QueryBuilder<Timeseries, TimeseriesBuilder> {

    public TimeseriesBuilder(Timeseries timeseries) {
        super(timeseries);
    }

    public TimeseriesBuilder setDescending(boolean descending) {
        query.setDescending(descending);

        return this;
    }

    public TimeseriesBuilder addInterval(Interval interval) {
        Intervals intervals = query.getIntervals();
        if (Objects.isNull(intervals)) {
            intervals = new Intervals();

            query.setIntervals(intervals);
        }

        intervals.add(interval);

        return this;
    }

    public TimeseriesBuilder setGranularity(Granularity granularity) {
        query.setGranularity(granularity);

        return this;
    }

    public TimeseriesBuilder setFilter(Filter filter) {
        query.setFilter(filter);

        return this;
    }

    public TimeseriesBuilder addAggregator(Aggregator aggregator) {
        List<Aggregator> aggregators = query.getAggregations();
        if (Objects.isNull(aggregators)) {
            aggregators = new ArrayList<>();

            query.setAggregations(aggregators);
        }

        aggregators.add(aggregator);

        return this;
    }

    public TimeseriesBuilder addPostAggregator(PostAggregator postAggregator) {
        List<PostAggregator> postAggregators = query.getPostAggregations();
        if (Objects.isNull(postAggregators)) {
            postAggregators = new ArrayList<>();

            query.setPostAggregations(postAggregators);
        }

        postAggregators.add(postAggregator);

        return this;
    }

}
