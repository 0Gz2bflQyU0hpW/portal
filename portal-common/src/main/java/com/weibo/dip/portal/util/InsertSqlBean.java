package com.weibo.dip.portal.util;

/**
 * @author : lihx create date : 2013-12-16
 */
public class InsertSqlBean {
    private static final String INSERT = "INSERT INTO ";
    private static final String VALUE = " VALUES ";
    private static final String LEFT = " ( ";
    private static final String RIGHT = " ) ";

    private StringBuilder columns = new StringBuilder();
    private StringBuilder values = new StringBuilder();
    private StringBuilder prepared = new StringBuilder();
    private StringBuilder table = new StringBuilder();
    private boolean preparedFlag = false;

    /**
     * 增加列,如果为预编辑，同时加？号
     *
     * @param column
     * @return
     */
    public InsertSqlBean addColumn(String column) {
        if (this.columns.length() != 0) {
            this.columns.append(" , ");
            this.prepared.append(",");
        }
        this.columns.append(column);
        this.prepared.append(" ? ");
        return this;
    }

    /**
     * 增加seq
     *
     * @param column
     * @param value
     * @return
     */
    public InsertSqlBean addSeqColumn(String column, String value) {
        if (this.columns.length() != 0) {
            this.columns.append(" , ");
            this.prepared.append(",");
        }
        this.columns.append(column);
        this.prepared.append(value);
        return this;
    }

    public InsertSqlBean addValues(Object value) {
        return addValues(value, false);
    }

    /**
     * 加值，判断日期类型
     *
     * @param value
     * @param isDate
     * @return
     */
    public InsertSqlBean addValues(Object value, boolean isDate) {
        if (this.values.length() != 0) {
            this.values.append(" , ");
        }
        String newValue = value.toString();
        if (value instanceof String)
            newValue = "'" + value.toString() + "'";
        if (isDate) {
            this.values.append(" to_date(").append(value).append(",'yyyy-mm-dd hh24:mi:ss')");
        } else
            this.values.append(newValue);
        return this;
    }

    /**
     * 表名
     *
     * @param tableName 表名
     * @return
     */
    public InsertSqlBean addTableName(String tableName) {
        if (this.table.length() != 0) {
            this.table.append(",");
        }
        this.table.append(tableName);
        return this;
    }

    public String toString() {
        StringBuilder temp = new StringBuilder();
        temp.append(INSERT).append(table).append(LEFT).append(columns).append(RIGHT).append(VALUE).append(LEFT);
        if (preparedFlag) {
            temp.append(prepared);
        } else {
            temp.append(values);
        }
        temp.append(RIGHT);
        return temp.toString();
    }

    public boolean isPreparedFlag() {
        return preparedFlag;
    }

    public void setPreparedFlag(boolean preparedFlag) {
        this.preparedFlag = preparedFlag;
    }
}
