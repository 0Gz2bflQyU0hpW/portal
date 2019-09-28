package com.weibo.dip.data.platform.datacubic.druid.dimension;

/**
 * Created by yurun on 17/2/14.
 */
public class DefaultDimension extends Dimension {

    private static final String DEFAULT = "default";

    private String dimension;

    private String outputName;

    public DefaultDimension() {
        super(DEFAULT);
    }

    public DefaultDimension(String dimension) {
        super(DEFAULT);

        this.dimension = dimension;
        this.outputName = dimension;
    }

    public DefaultDimension(String dimension, String outputName) {
        super(DEFAULT);

        this.dimension = dimension;
        this.outputName = outputName;
    }

    @Override
    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public String getOutputName() {
        return outputName;
    }

    public void setOutputName(String outputName) {
        this.outputName = outputName;
    }

}
