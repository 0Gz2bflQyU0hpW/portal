package com.weibo.dip.data.platform.kafka;

import kafka.cluster.Broker;

import java.util.List;

/**
 * Created by yurun on 17/7/16.
 */
public class Partition {

    private String topic;

    private int id;

    private long offset;

    private Broker leader;

    private List<Broker> replicas;

    private List<Broker> isr;

    public Partition(String topic, int id, long offset, Broker leader, List<Broker> replicas,
                     List<Broker> isr) {
        this.topic = topic;
        this.id = id;
        this.offset = offset;
        this.leader = leader;
        this.replicas = replicas;
        this.isr = isr;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public Broker getLeader() {
        return leader;
    }

    public void setLeader(Broker leader) {
        this.leader = leader;
    }

    public List<Broker> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<Broker> replicas) {
        this.replicas = replicas;
    }

    public List<Broker> getIsr() {
        return isr;
    }

    public void setIsr(List<Broker> isr) {
        this.isr = isr;
    }

    @Override
    public String toString() {
        return "Partition{" +
            "topic='" + topic + '\'' +
            ", id=" + id +
            ", offset=" + offset +
            ", leader=" + leader +
            ", replicas=" + replicas +
            ", isr=" + isr +
            '}';
    }

}
