package com.weibo.dip.ml.godeyes;

import java.io.Serializable;

/**
 * Created by yurun on 17/7/7.
 */
public class Record implements Serializable {

    private String type;
    private String service;
    private long timestamp;
    private long value;

    public Record() {
    }

    public Record(String type, String service, long timestamp, long value) {
        this.type = type;
        this.service = service;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Record{" +
            "type='" + type + '\'' +
            ", service='" + service + '\'' +
            ", timestamp=" + timestamp +
            ", value=" + value +
            '}';
    }

}
