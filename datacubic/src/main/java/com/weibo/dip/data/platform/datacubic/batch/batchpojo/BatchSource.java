package com.weibo.dip.data.platform.datacubic.batch.batchpojo;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by xiaoyu on 2017/6/8.
 */
public class BatchSource implements Serializable {
    private String inputPathRoot;
    private String tableName;
    private String format;
    private String[] columns;
    private StructType schema;
    private String regex;

    public BatchSource(){}

    public BatchSource(String inputPathRoot, String tableName,String format, String[] columns,String regex) {
        this.inputPathRoot = inputPathRoot;
        this.tableName = tableName;
        this.format = format;
        this.columns = columns;
        this.schema = createSchema(columns);
        this.regex = regex;
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

    public String getInputPathRoot() {
        return inputPathRoot;
    }

    public void setInputPathRoot(String inputPathRoot) {
        this.inputPathRoot = inputPathRoot;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public StructType getSchema() {
        return schema;
    }

    public void setSchema(StructType schema) {
        this.schema = schema;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
    }

    @Override
    public String toString() {
        return "BatchSource{" +
                "inputPathRoot='" + inputPathRoot + '\'' +
                ", tableName='" + tableName + '\'' +
                ", format='" + format + '\'' +
                ", columns=" + Arrays.toString(columns) +
                ", schema=" + schema +
                ", regex='" + regex + '\'' +
                '}';
    }
}
