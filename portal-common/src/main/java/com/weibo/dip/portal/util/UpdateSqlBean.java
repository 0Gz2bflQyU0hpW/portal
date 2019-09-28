package com.weibo.dip.portal.util;

/**
 * @author : lihx create date : 2013-12-16
 */
public class UpdateSqlBean {
    private static final String UPDATE = "UPDATE ";
    private static final String SET = " SET ";
    private static final String WHERE = " WHERE 1=1 ";
    private static final String AND = " AND ";
    private static final String BLANK = " ";
    private static final String IN = " IN ";
    private static final String OR = " OR ";

    private StringBuilder condition = new StringBuilder();
    private StringBuilder prepared = new StringBuilder();
    private StringBuilder table = new StringBuilder();
    private boolean preparedFlag = false;
    private StringBuilder orCondition = new StringBuilder();
    private StringBuilder columns = new StringBuilder();


    /**
     * 增加修改列
     *
     * @param column
     * @return
     */
    public UpdateSqlBean addColumn(String column) {
        if (this.columns.length() != 0) {
            this.columns.append(" , ");
        }
        this.columns.append(column);
        return this;
    }

    public UpdateSqlBean addPreparedColumn(String column) {
        this.preparedFlag = true;
        if (this.columns.length() != 0) {
            this.columns.append(" , ");
        }
        this.columns.append(column).append(" = ? ");
        ;
        return this;
    }

    /**
     * 增加修改列带别名
     *
     * @param column
     * @param value
     */
    public UpdateSqlBean addColumn(String column, Object value) {
        return addColumn(column, value, false);
    }

    public UpdateSqlBean addColumn(String column, Object value, boolean isDate) {
        if (this.columns.length() == 0) {
            this.columns.append(WHERE);
        }
        this.columns.append(AND).append(column).append("=");

        String newValue = value.toString();
        if (value instanceof String) {
            newValue = "'" + value.toString() + "'";
        }

        if (isDate) {
            this.columns.append(" to_date(").append(newValue).append(",'yyyy-mm-dd hh24:mi:ss')");
        } else {
            this.columns.append(newValue);
        }
        return this;
    }


    /**
     * 表名
     *
     * @param tableName 表名
     * @return
     */
    public UpdateSqlBean addTableName(String tableName) {
        if (this.table.length() != 0) {
            this.table.append(",");
        }
        this.table.append(tableName);
        return this;
    }

    /**
     * 插入条件，参数格式为 col=value
     *
     * @param str
     */
    public UpdateSqlBean addCondition(String str) {
        if (this.condition.length() == 0)
            this.condition.append(WHERE);
        this.condition.append(AND).append(str);
        return this;
    }

    /**
     * 增加prepared条件
     *
     * @param str
     * @return
     */
    public UpdateSqlBean addPreparedCondition(String str) {
        this.preparedFlag = true;
        if (this.condition.length() == 0)
            this.condition.append(WHERE);
        this.condition.append(AND).append(str).append(" = ? ");
        return this;
    }

    /**
     * 增加prepared条件
     *
     * @param str
     * @return
     */
    public UpdateSqlBean addPreparedOrCondition(String str) {
        this.preparedFlag = true;
        if (this.orCondition.length() > 0)
            this.orCondition.append(OR);
        this.orCondition.append(str).append(" = ? ");
        return this;
    }

    /**
     * 结束prepared 的or的sql拼接
     *
     * @return
     */
    public UpdateSqlBean closePreparedOrCondition() {
        this.preparedFlag = true;
        if (orCondition.length() > 0) {
            StringBuilder temp = new StringBuilder();
            temp.append(" ( ").append(orCondition).append(" ) ");
            addCondition(temp.toString());
        }
        orCondition = new StringBuilder();
        return this;
    }

    /**
     * 拼接条件
     *
     * @param str   字段
     * @param value 字段值
     */
    public UpdateSqlBean addCondition(String str, Object value) {
        return this.addCondition(str, value, false);

    }

    /**
     * 拼接条件
     *
     * @param str    字段
     * @param value  字段值
     * @param isDate 是否为日期
     */
    public UpdateSqlBean addCondition(String str, Object value, boolean isDate) {
        if (this.condition.length() == 0) {
            this.condition.append(WHERE);
        }
        this.condition.append(AND).append(str).append("=");

        String newValue = value.toString();
        if (value instanceof String) {
            newValue = "'" + value.toString() + "'";
        }

        if (isDate) {
            this.condition.append(" to_date(").append(newValue).append(",'yyyy-mm-dd hh24:mi:ss')");
        } else {
            this.condition.append(newValue);
        }
        return this;
    }

    /**
     * sql 语句 条件等于子查询
     *
     * @param str
     * @param value
     * @return
     */
    public UpdateSqlBean addEqSubCondition(String str, Object value) {
        if (this.condition.length() == 0) {
            this.condition.append(WHERE);
        }
        this.condition.append(AND).append(str).append(" = (").append(value).append(")");
        return this;
    }

    /**
     * sql 语句in 查询
     *
     * @param str
     * @param value
     * @return
     */
    public UpdateSqlBean addInSubCondition(String str, Object value) {
        if (this.condition.length() == 0) {
            this.condition.append(WHERE);
        }
        this.condition.append(AND).append(str).append(IN).append("(").append(value).append(")");
        return this;
    }

    public String toString() {
        StringBuilder temp = new StringBuilder();
        temp.append(UPDATE).append(table).append(SET).append(columns).append(condition);
        return temp.toString();
    }

    public boolean isPreparedFlag() {
        return preparedFlag;
    }

    public void setPreparedFlag(boolean preparedFlag) {
        this.preparedFlag = preparedFlag;
    }
}
