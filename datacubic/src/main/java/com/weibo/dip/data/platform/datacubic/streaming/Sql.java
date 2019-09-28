package com.weibo.dip.data.platform.datacubic.streaming;

import java.io.Serializable;

/**
 * Created by yurun on 17/1/17.
 */
public class Sql implements Serializable {

    private String sql;

    public Sql() {
    }

    public Sql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    @Override
    public String toString() {
        return "Sql{" +
            "sql='" + sql + '\'' +
            '}';
    }

}
