package com.weibo.dip.data.platform.datacubic.streaming;

import com.weibo.dip.data.platform.datacubic.streaming.mapper.RowMapper;

import java.io.Serializable;

/**
 * Created by yurun on 17/1/17.
 */
public class Sink implements Serializable {

    private String servers;

    private String topic;

    private RowMapper mapper;

    public Sink() {

    }

    public Sink(String servers, String topic, RowMapper mapper) {
        this.servers = servers;
        this.topic = topic;
        this.mapper = mapper;
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

    @Override
    public String toString() {
        return "Sink{" +
            "servers='" + servers + '\'' +
            ", topic='" + topic + '\'' +
            ", mapper=" + mapper.getClass().getName() +
            '}';
    }

}
