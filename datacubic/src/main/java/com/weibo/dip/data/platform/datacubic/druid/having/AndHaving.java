package com.weibo.dip.data.platform.datacubic.druid.having;

/**
 * Created by yurun on 17/3/29.
 */
public class AndHaving extends Having {

    private static final String AND = "and";

    private Having[] havingSpecs;

    public AndHaving(Having... havingSpecs) {
        super(AND);

        this.havingSpecs = havingSpecs;
    }

    public Having[] getHavingSpecs() {
        return havingSpecs;
    }

    public void setHavingSpecs(Having[] havingSpecs) {
        this.havingSpecs = havingSpecs;
    }

}
