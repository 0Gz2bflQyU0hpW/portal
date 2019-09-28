package com.weibo.dip.data.platform.datacubic.streaming;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yurun on 17/1/17.
 */
public class Source implements Serializable {

    public static final String RECEIVER = "receiver";

    public static final String DIRECT = "direct";

    private long duration;

    private String approach;

    private String zkQuorums;

    private String brokers;

    private String topic;

    private boolean trim = true;

    private String format;

    private String regex;

    private String[] columns;

    private String consumerGroup;

    private int receivers;

    private String table;

    private StructType schema;

    public Source() {
    }

    public Source(long duration, String zkQuorums, String topic, boolean trim, String format, String regex, String[] columns, String consumerGroup, int receivers, String table) {
        this.duration = duration;
        this.approach = RECEIVER;
        this.zkQuorums = zkQuorums;
        this.topic = topic;
        this.trim = trim;
        this.format = format;
        this.regex = regex;
        this.columns = columns;
        this.consumerGroup = consumerGroup;
        this.receivers = receivers;
        this.table = table;
        this.schema = createSchema(columns);
    }

    public Source(long duration, String brokers, String topic, boolean trim, String format, String regex, String[] columns, String table) {
        this.duration = duration;
        this.approach = DIRECT;
        this.brokers = brokers;
        this.topic = topic;
        this.trim = trim;
        this.format = format;
        this.regex = regex;
        this.columns = columns;
        this.table = table;
        this.schema = createSchema(columns);
    }

    private StructType createSchema(String[] columns) {
        if (ArrayUtils.isEmpty(columns)) {
            return null;
        }

        List<StructField> fields = new ArrayList<>();

        for (String columnName : columns) {
            fields.add(DataTypes.createStructField(columnName, DataTypes.StringType, true));
        }

        return DataTypes.createStructType(fields);
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public String getApproach() {
        return approach;
    }

    public void setApproach(String approach) {
        this.approach = approach;
    }

    public boolean isReceiver() {
        return RECEIVER.equals(approach);
    }

    public boolean isDirect() {
        return DIRECT.equals(approach);
    }

    public String getZkQuorums() {
        return zkQuorums;
    }

    public void setZkQuorums(String zkQuorums) {
        this.zkQuorums = zkQuorums;
    }

    public String getBrokers() {
        return brokers;
    }

    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isTrim() {
        return trim;
    }

    public void setTrim(boolean trim) {
        this.trim = trim;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public int getReceivers() {
        return receivers;
    }

    public void setReceivers(int receivers) {
        this.receivers = receivers;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public StructType getSchema() {
        return schema;
    }

    public void setSchema(StructType schema) {
        this.schema = schema;
    }

    @Override
    public String toString() {
        return "Source{" +
            "duration=" + duration +
            ", approach='" + approach + '\'' +
            ", zkQuorums='" + zkQuorums + '\'' +
            ", brokers='" + brokers + '\'' +
            ", topic='" + topic + '\'' +
            ", trim=" + trim +
            ", format='" + format + '\'' +
            ", regex='" + regex + '\'' +
            ", columns=" + Arrays.toString(columns) +
            ", consumerGroup='" + consumerGroup + '\'' +
            ", receivers=" + receivers +
            ", table='" + table + '\'' +
            ", schema=" + schema +
            '}';
    }

}
