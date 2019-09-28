package com.weibo.dip.data.platform.datacubic.druid.query;

import com.weibo.dip.data.platform.datacubic.druid.aggregation.Aggregator;
import com.weibo.dip.data.platform.datacubic.druid.aggregation.post.PostAggregator;
import com.weibo.dip.data.platform.datacubic.druid.filter.Filter;

import java.util.List;

/**
 * Created by yurun on 17/2/20.
 */
public class Timeseries extends Query {

    private static final String TIMESERIES = "timeseries";

    private boolean descending = false;

    private Intervals intervals;

    private Granularity granularity;

    private Filter filter;

    private List<Aggregator> aggregations;

    private List<PostAggregator> postAggregations;

    public Timeseries() {
        queryType = TIMESERIES;
    }

    public boolean isDescending() {
        return descending;
    }

    public void setDescending(boolean descending) {
        this.descending = descending;
    }

    public Intervals getIntervals() {
        return intervals;
    }

    public void setIntervals(Intervals intervals) {
        this.intervals = intervals;
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

}
