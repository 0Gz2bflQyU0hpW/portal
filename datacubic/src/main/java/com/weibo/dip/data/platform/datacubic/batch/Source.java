package com.weibo.dip.data.platform.datacubic.batch;

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

    private String[] inputPath;

    private String format;

    private String regex;

    private String[] columns;

    private String table;

    private StructType schema;

    private String srcColumnName = null;

    public Source() {
    }

    public Source(String[] inputPath, String format, String regex, String[] columns, String table) {
        this.inputPath = inputPath;
        this.format = format;
        this.regex = regex;
        this.columns = columns;
        this.table = table;
        this.schema = createSchema(columns);
    }

    public Source(String[] inputPath, String format, String regex, String[] columns, String table, String srcColumnName) {
        this.inputPath = inputPath;
        this.format = format;
        this.regex = regex;
        this.columns = columns;
        this.table = table;
        this.schema = createSchema(columns);
        this.srcColumnName = srcColumnName;
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

    public String[] getInputPath() {
        return inputPath;
    }

    public void setInputPath(String[] inputPath) {
        this.inputPath = inputPath;
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

    public String getSrcColumnName() {
        return srcColumnName;
    }

    public void setSrcColumnName(String srcColumnName) {
        this.srcColumnName = srcColumnName;
    }

    @Override
    public String toString() {
        return "Source{" +
                "inputPath='" + Arrays.toString(inputPath) + '\'' +
                ", format='" + format + '\'' +
                ", regex='" + regex + '\'' +
                ", columns=" + Arrays.toString(columns) +
                ", table='" + table + '\'' +
                ", schema=" + schema +
                ", srcColumnName='" + srcColumnName + '\'' +
                '}';
    }
}
