package com.weib.dip.data.platform.services.client.model;

import com.google.common.base.Objects;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.io.Serializable;

/**
 * Created by ftx on 2016/12/22.
 */
public class HApplicationId implements Serializable{
    private int id;
    private long clusterTimestamp;

    public HApplicationId() {
    }
    public HApplicationId(int id, long clusterTimestamp) {
        this.id = id;
        this.clusterTimestamp = clusterTimestamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getClusterTimestamp() {
        return clusterTimestamp;
    }

    public void setClusterTimestamp(long clusterTimestamp) {
        this.clusterTimestamp = clusterTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HApplicationId that = (HApplicationId) o;
        return Objects.equal(id, that.id) &&
                Objects.equal(clusterTimestamp, that.clusterTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, clusterTimestamp);
    }

    @Override
    public String toString() {
        return "HApplicationId{" +
                "id=" + id +
                ", clusterTimestamp=" + clusterTimestamp +
                '}';
    }
}
