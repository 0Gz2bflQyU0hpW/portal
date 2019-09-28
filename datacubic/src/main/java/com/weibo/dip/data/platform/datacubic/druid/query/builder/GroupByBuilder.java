package com.weibo.dip.data.platform.datacubic.druid.query.builder;

import com.weibo.dip.data.platform.datacubic.druid.aggregation.Aggregator;
import com.weibo.dip.data.platform.datacubic.druid.aggregation.post.PostAggregator;
import com.weibo.dip.data.platform.datacubic.druid.dimension.Dimension;
import com.weibo.dip.data.platform.datacubic.druid.filter.Filter;
import com.weibo.dip.data.platform.datacubic.druid.having.Having;
import com.weibo.dip.data.platform.datacubic.druid.limit.LimitSpec;
import com.weibo.dip.data.platform.datacubic.druid.query.Granularity;
import com.weibo.dip.data.platform.datacubic.druid.query.GroupBy;
import com.weibo.dip.data.platform.datacubic.druid.query.Interval;
import com.weibo.dip.data.platform.datacubic.druid.query.Intervals;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created by yurun on 17/1/24.
 */
public class GroupByBuilder extends QueryBuilder<GroupBy, GroupByBuilder> {

    public GroupByBuilder(GroupBy groupBy) {
        super(groupBy);
    }

    public GroupByBuilder addDimension(Dimension dimension) {
        List<Dimension> dimensions = query.getDimensions();
        if (Objects.isNull(dimensions)) {
            dimensions = new ArrayList<>();

            query.setDimensions(dimensions);
        }

        dimensions.add(dimension);

        return this;
    }

    public GroupByBuilder setLimitSpec(LimitSpec limitSpec) {
        query.setLimitSpec(limitSpec);

        return this;
    }

    public GroupByBuilder setHaving(Having having) {
        query.setHaving(having);

        return this;
    }

    public GroupByBuilder setGranularity(Granularity granularity) {
        query.setGranularity(granularity);

        return this;
    }

    public GroupByBuilder setFilter(Filter filter) {
        query.setFilter(filter);

        return this;
    }

    public GroupByBuilder addAggregator(Aggregator aggregator) {
        List<Aggregator> aggregators = query.getAggregations();
        if (Objects.isNull(aggregators)) {
            aggregators = new ArrayList<>();

            query.setAggregations(aggregators);
        }

        aggregators.add(aggregator);

        return this;
    }

    public GroupByBuilder addPostAggregator(PostAggregator postAggregator) {
        List<PostAggregator> postAggregators = query.getPostAggregations();
        if (Objects.isNull(postAggregators)) {
            postAggregators = new ArrayList<>();

            query.setPostAggregations(postAggregators);
        }

        postAggregators.add(postAggregator);

        return this;
    }

    public GroupByBuilder addInterval(Interval interval) {
        Intervals intervals = query.getIntervals();
        if (Objects.isNull(intervals)) {
            intervals = new Intervals();

            query.setIntervals(intervals);
        }

        intervals.add(interval);

        return this;
    }

}
