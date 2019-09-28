package com.weibo.dip.data.platform.datacubic.druid.query;

import java.util.UUID;

/**
 * Created by yurun on 17/1/24.
 */
public class Context {

    private long timeout = Long.MAX_VALUE;

    private int priority = 0;

    private String queryId = UUID.randomUUID().toString();

    private boolean useCache = true;

    private boolean populateCache = true;

    private boolean bySegment = false;

    private boolean finalize = true;

    private String chunkPeriod = "P0D";

    private long minTopNThreshold = 1000;

    private long maxResults = Integer.MAX_VALUE;

    private long maxIntermediateRows = Integer.MAX_VALUE;

    private boolean groupByIsSingleThreaded = false;

    public Context() {
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public boolean isUseCache() {
        return useCache;
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    public boolean isPopulateCache() {
        return populateCache;
    }

    public void setPopulateCache(boolean populateCache) {
        this.populateCache = populateCache;
    }

    public boolean isBySegment() {
        return bySegment;
    }

    public void setBySegment(boolean bySegment) {
        this.bySegment = bySegment;
    }

    public boolean isFinalize() {
        return finalize;
    }

    public void setFinalize(boolean finalize) {
        this.finalize = finalize;
    }

    public String getChunkPeriod() {
        return chunkPeriod;
    }

    public void setChunkPeriod(String chunkPeriod) {
        this.chunkPeriod = chunkPeriod;
    }

    public long getMinTopNThreshold() {
        return minTopNThreshold;
    }

    public void setMinTopNThreshold(long minTopNThreshold) {
        this.minTopNThreshold = minTopNThreshold;
    }

    public long getMaxResults() {
        return maxResults;
    }

    public void setMaxResults(long maxResults) {
        this.maxResults = maxResults;
    }

    public long getMaxIntermediateRows() {
        return maxIntermediateRows;
    }

    public void setMaxIntermediateRows(long maxIntermediateRows) {
        this.maxIntermediateRows = maxIntermediateRows;
    }

    public boolean isGroupByIsSingleThreaded() {
        return groupByIsSingleThreaded;
    }

    public void setGroupByIsSingleThreaded(boolean groupByIsSingleThreaded) {
        this.groupByIsSingleThreaded = groupByIsSingleThreaded;
    }

}
