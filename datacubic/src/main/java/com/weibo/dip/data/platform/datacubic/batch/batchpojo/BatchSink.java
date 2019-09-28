package com.weibo.dip.data.platform.datacubic.batch.batchpojo;

import com.weibo.dip.data.platform.datacubic.streaming.mapper.RowMapper;

import java.io.Serializable;

/**
 * Created by xiaoyu on 2017/6/8.
 */
public class BatchSink implements Serializable {

    private RowMapper mapper;

    private String direction;

    private String hdfsPath;

    private String servers;

    private String topic;

    public BatchSink() {

    }

    public BatchSink(String servers, String topic) {
        this.servers = servers;
        this.topic = topic;
    }

    public BatchSink(RowMapper mapper, String direction, String hdfsPath, String servers, String topic) {
        this.mapper = mapper;
        this.direction = direction;
        this.hdfsPath = hdfsPath;
        this.servers = servers;
        this.topic = topic;
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public RowMapper getMapper() {
        return mapper;
    }

    public void setMapper(RowMapper mapper) {
        this.mapper = mapper;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }

    @Override
    public String toString() {
        return "BatchSink{" +
                "mapper=" + mapper +
                ", direction='" + direction + '\'' +
                ", hdfsPath='" + hdfsPath + '\'' +
                ", servers='" + servers + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }
}