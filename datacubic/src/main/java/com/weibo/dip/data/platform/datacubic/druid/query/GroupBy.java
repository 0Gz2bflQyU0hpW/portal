package com.weibo.dip.data.platform.datacubic.druid.query;

import com.weibo.dip.data.platform.datacubic.druid.aggregation.Aggregator;
import com.weibo.dip.data.platform.datacubic.druid.aggregation.post.PostAggregator;
import com.weibo.dip.data.platform.datacubic.druid.dimension.Dimension;
import com.weibo.dip.data.platform.datacubic.druid.filter.Filter;
import com.weibo.dip.data.platform.datacubic.druid.having.Having;
import com.weibo.dip.data.platform.datacubic.druid.limit.LimitSpec;

import java.util.List;

/**
 * Created by yurun on 17/2/20.
 */
public class GroupBy extends Query {

    private static final String GROUPBY = "groupBy";

    private List<Dimension> dimensions;

    private LimitSpec limitSpec;

    private Having having;

    private Granularity granularity;

    private Filter filter;

    private List<Aggregator> aggregations;

    private List<PostAggregator> postAggregations;

    private Intervals intervals;

    public GroupBy() {
        queryType = GROUPBY;
    }

    public List<Dimension> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<Dimension> dimensions) {
        this.dimensions = dimensions;
    }

    public LimitSpec getLimitSpec() {
        return limitSpec;
    }

    public void setLimitSpec(LimitSpec limitSpec) {
        this.limitSpec = limitSpec;
    }

    public Having getHaving() {
        return having;
    }

    public void setHaving(Having having) {
        this.having = having;
    }

    public Granularity getGranularity() {
        return granularity;
    }

    public void setGranularity(Granularity granularity) {
        this.granularity = granularity;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public List<Aggregator> getAggregations() {
        return aggregations;
    }

    public void setAggregations(List<Aggregator> aggregations) {
        this.aggregations = aggregations;
    }

    public List<PostAggregator> getPostAggregations() {
        return postAggregations;
    }

    public void setPostAggregations(List<PostAggregator> postAggregations) {
        this.postAggregations = postAggregations;
    }

    public Intervals getIntervals() {
        return intervals;
    }

    public void setIntervals(Intervals intervals) {
        this.intervals = intervals;
    }

}
