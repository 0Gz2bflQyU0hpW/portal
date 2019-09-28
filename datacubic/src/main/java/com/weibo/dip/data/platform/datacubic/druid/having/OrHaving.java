package com.weibo.dip.data.platform.datacubic.druid.having;

/**
 * Created by yurun on 17/3/29.
 */
public class OrHaving extends Having {

    private static final String OR = "or";

    private Having[] havingSpecs;

    public OrHaving(Having... havingSpecs) {
        super(OR);

        this.havingSpecs = havingSpecs;
    }

    public Having[] getHavingSpecs() {
        return havingSpecs;
    }

    public void setHavingSpecs(Having[] havingSpecs) {
        this.havingSpecs = havingSpecs;
    }

}
