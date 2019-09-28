package com.weibo.dip.data.platform.datacubic.druid.aggregation.post;

/**
 * Created by yurun on 17/3/29.
 */
public class FieldAccessorPostAggregator extends PostAggregator {

    private static final String FIELD_ACCESS = "fieldAccess";

    private String fieldName;

    public FieldAccessorPostAggregator(String fieldName) {
        super(FIELD_ACCESS, fieldName);

        this.fieldName = fieldName;
    }

    public FieldAccessorPostAggregator(String name, String fieldName) {
        super(FIELD_ACCESS, name);

        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

}
