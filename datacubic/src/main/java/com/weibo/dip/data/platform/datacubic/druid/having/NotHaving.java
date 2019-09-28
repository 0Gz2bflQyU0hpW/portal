package com.weibo.dip.data.platform.datacubic.druid.having;

/**
 * Created by yurun on 17/3/29.
 */
public class NotHaving extends Having {

    private static final String NOT = "not";

    private Having havingSpec;

    public NotHaving(Having havingSpec) {
        super(NOT);

        this.havingSpec = havingSpec;
    }

    public Having getHavingSpec() {
        return havingSpec;
    }

    public void setHavingSpec(Having havingSpec) {
        this.havingSpec = havingSpec;
    }

}
