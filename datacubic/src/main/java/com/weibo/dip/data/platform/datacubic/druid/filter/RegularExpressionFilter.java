package com.weibo.dip.data.platform.datacubic.druid.filter;

/**
 * Created by yurun on 17/1/23.
 */
public class RegularExpressionFilter extends Filter {

    private static final String REGTEX = "regex";

    private String dimension;

    private String pattern;

    public RegularExpressionFilter(String dimension, String pattern) {
        super(REGTEX);

        this.dimension = dimension;
        this.pattern = pattern;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

}
