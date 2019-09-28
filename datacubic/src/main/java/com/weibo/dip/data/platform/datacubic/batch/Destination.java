package com.weibo.dip.data.platform.datacubic.batch;

import com.weibo.dip.data.platform.datacubic.streaming.mapper.RowMapper;

/**
 * Created by xiaoyu on 2017/3/8.
 */
public class Destination {

    private String outputPath;

    private RowMapper mapper;

    public Destination() {
    }

    public Destination(String outputPath, RowMapper mapper) {
        this.outputPath = outputPath;
        this.mapper = mapper;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public RowMapper getMapper() {
        return mapper;
    }

    public void setMapper(RowMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public String toString() {
        return "Output{" +
                "outputPath='" + outputPath + '\'' +
                ", mapper=" + mapper +
                '}';
    }
}
