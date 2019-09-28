package com.weibo.dip.data.platform.datacubic.druid.query;

import com.weibo.dip.data.platform.commons.util.GsonUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yurun on 17/2/14.
 */
public class Row {

    private String rawData;

    private Map<String, Object> data;

    public Row() {

    }

    public Row(String rawData) {
        this.rawData = rawData;

        data = GsonUtil.fromJson(rawData, GsonUtil.GsonType.OBJECT_MAP_TYPE);
    }

    public Row(Map<String, Object> data) {
        this.data = data;
    }

    public void set(String name, Object value) {
        if (data == null) {
            data = new HashMap<>();
        }

        data.put(name, value);
    }

    public String getRawData() {
        return rawData;
    }

    public Object get(String name) {
        return data.get(name);
    }

    public String getString(String name) {
        return (String) get(name);
    }

    public int getInt(String name) {
        Object value = get(name);

        if (value instanceof Float) {
            return ((Float) value).intValue();
        } else if (value instanceof Double) {
            return ((Double) value).intValue();
        } else {
            return (int) get(name);
        }
    }

    public long getLong(String name) {
        Object value = get(name);

        if (value instanceof Float) {
            return ((Float) value).longValue();
        } else if (value instanceof Double) {
            return ((Double) value).longValue();
        } else {
            return (long) get(name);
        }
    }

    public float getFloat(String name) {
        return (float) get(name);
    }

    public double getDouble(String name) {
        return (double) get(name);
    }

    @Override
    public String toString() {
        return GsonUtil.toJson(data);
    }

}
