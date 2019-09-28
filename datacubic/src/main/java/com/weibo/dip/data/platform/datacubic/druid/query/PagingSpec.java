package com.weibo.dip.data.platform.datacubic.druid.query;

import java.util.Map;

/**
 * Created by yurun on 17/1/23.
 */
public class PagingSpec {

    private Map<String, Integer> pagingIdentifiers;

    private int threshold;

    public PagingSpec() {
        threshold = Integer.MAX_VALUE;
    }

    public PagingSpec(Map<String, Integer> pagingIdentifiers, int threshold) {
        this.pagingIdentifiers = pagingIdentifiers;
        this.threshold = threshold;
    }

    public Map<String, Integer> getPagingIdentifiers() {
        return pagingIdentifiers;
    }

    public void setPagingIdentifiers(Map<String, Integer> pagingIdentifiers) {
        this.pagingIdentifiers = pagingIdentifiers;
    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

}
