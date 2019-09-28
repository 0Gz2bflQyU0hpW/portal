package com.weib.dip.data.platform.services.client.model;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * Created by ftx on 2016/12/2.
 */
public class SaveStrategy implements Serializable, Comparable {
    private long start;
    private long end;
    private int replication;

    public SaveStrategy() {
    }

    public SaveStrategy(long start, long end, int replication) {
        this.start = start;
        this.end = end;
        this.replication = replication;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public int getReplication() {
        return replication;
    }

    public void setReplication(int replication) {
        this.replication = replication;
    }

    @Override
    public int compareTo(Object object) {
        SaveStrategy temp = (SaveStrategy) object;
        if (this.start - temp.getStart() > 0) {
            if (this.end - temp.getEnd() > 0) {
                return 1;
            }
        } else
            return -1;
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SaveStrategy that = (SaveStrategy) o;
        return Objects.equal(start, that.start) &&
                Objects.equal(end, that.end) &&
                Objects.equal(replication, that.replication);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(start, end, replication);
    }

    @Override
    public String toString() {
        return "SaveStrategy{" +
                "start=" + start +
                ", end=" + end +
                ", replication=" + replication +
                '}';
    }
}
