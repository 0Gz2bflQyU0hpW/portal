package com.weibo.dip.portal.util;

/**
 * @author : lihx create date : 2013-12-16
 */
public class SelectSqlBean {

    private static final String WHERE = " WHERE 1=1 ";
    private static final String SELECT = " SELECT ";
    private static final String JOIN = " JOIN ";
    private static final String LEFT_JOIN = " LEFT JOIN ";
    private static final String RIGHT_JOIN = " RIGHT JOIN ";
    private static final String GROUP_BY = " GROUP BY ";
    private static final String UPDATE = "UPDATE ";
    private static final String FROM = " FROM ";
    private static final String AND = " AND ";
    private static final String OR = " OR ";
    private static final String AS = " AS ";
    private static final String ON = " ON ";
    private static final String BLANK = " ";
    private static final String ORDER_BY = " ORDER BY ";
    private static final String MORE = ">";
    private static final String LESS = "<";
    private static final String MORE_E = ">=";
    private static final String LESS_E = "<=";

    private StringBuilder sql = new StringBuilder();
    private StringBuilder columns = new StringBuilder();
    private StringBuilder condition = new StringBuilder();
    private StringBuilder table = new StringBuilder();
    private StringBuilder joinCondition = new StringBuilder();
    private StringBuilder groupByCol = new StringBuilder();
    private StringBuilder orderByCol = new StringBuilder();
    private StringBuilder others = new StringBuilder();
    private StringBuilder prepared = new StringBuilder();
    private StringBuilder orCondition = new StringBuilder();
    private boolean preparedFlag = false;

    /**
     * 增加查询列
     *
     * @param column
     * @return
     */
    public SelectSqlBean addColumn(String column) {
        if (this.columns.length() != 0) {
            this.columns.append(" , ");
        }
        this.columns.append(column);
        return this;
    }

    /**
     * 增加查询列带别名
     *
     * @param column
     * @param alias
     */
    public SelectSqlBean addColumn(String column, String alias) {
        if (this.columns.length() != 0) {
            this.columns.append(",");
        }
        this.columns.append(column).append(AS).append(alias);
        return this;
    }

    public SelectSqlBean addTableName(String tableName) {
        if (this.table.length() != 0) {
            table.append(",");
        }
        table.append(tableName);
        return this;
    }

    public SelectSqlBean addTableName(String tableName, String alias) {
        if (this.table.length() != 0) {
            table.append(",");
        }
        table.append(tableName).append(BLANK).append(alias);
        return this;
    }

    public SelectSqlBean addSQLTable(String sql) {
        if (this.table.length() != 0) {
            table.append(" , ");
        }
        table.append(" ( ").append(sql).append(" ) ");
        return this;
    }

    public SelectSqlBean addSQLTable(String sql, String alias) {
        if (this.table.length() != 0) {
            table.append(" , ");
        }
        table.append(" ( ").append(sql).append(" ) ").append(BLANK).append(alias);
        return this;
    }

    /**
     * 插入条件，参数格式为 col=value
     *
     * @param str
     */
    public SelectSqlBean addCondition(String str) {
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
    public SelectSqlBean addPreparedCondition(String str) {
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
    public SelectSqlBean addPreparedOrCondition(String str) {
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
    public SelectSqlBean closePreparedOrCondition() {
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
    public SelectSqlBean addCondition(String str, Object value) {
        return this.addCondition(str, value, false);

    }

    /**
     * 大于
     *
     * @param str
     * @param value
     * @return
     */
    public SelectSqlBean addMoreCondition(String str, Object value) {
        return this.addMLCondition(str, value, MORE, false);

    }

    public SelectSqlBean addMoreCondition(String str, Object value, boolean isDate) {
        return this.addMLCondition(str, value, MORE, isDate);

    }

    /**
     * 大于等于
     *
     * @param str
     * @param value
     * @return
     */
    public SelectSqlBean addMoreEqCondition(String str, Object value) {
        return this.addMLCondition(str, value, MORE_E, false);

    }

    public SelectSqlBean addMoreEqCondition(String str, Object value, boolean isDate) {
        return this.addMLCondition(str, value, MORE_E, isDate);

    }

    /**
     * 小于
     *
     * @param str
     * @param value
     * @return
     */
    public SelectSqlBean addLessCondition(String str, Object value) {
        return this.addMLCondition(str, value, LESS, false);
    }

    public SelectSqlBean addLessCondition(String str, Object value, boolean isDate) {
        return this.addMLCondition(str, value, LESS, isDate);
    }

    /**
     * 小于等于
     *
     * @param str
     * @param value
     * @return
     */
    public SelectSqlBean addLessEqCondition(String str, Object value) {
        return this.addMLCondition(str, value, LESS_E, false);

    }

    public SelectSqlBean addLessEqCondition(String str, Object value, boolean isDate) {
        return this.addMLCondition(str, value, LESS_E, isDate);

    }

    /**
     * 大于小于等条件共用的私有方法
     *
     * @param column
     * @param value
     * @param m
     * @return
     */
    private SelectSqlBean addMLCondition(String column, Object value, String m, boolean isDate) {
        if (this.condition.length() == 0) {
            this.condition.append(WHERE);
        }
        this.condition.append(AND).append(column).append(m);
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
     * 拼接条件
     *
     * @param str    字段
     * @param value  字段值
     * @param isDate 是否为日期
     */
    public SelectSqlBean addCondition(String str, Object value, boolean isDate) {
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
    public SelectSqlBean addEqSubCondition(String str, Object value) {
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
    public SelectSqlBean addInSubCondition(String str, Object value) {
        if (this.condition.length() == 0) {
            this.condition.append(WHERE);
        }
        this.condition.append(AND).append(str).append(" in  (").append(value).append(")");
        return this;
    }

    public SelectSqlBean leftJoin(String tableName) {
        this.table.append(LEFT_JOIN).append(tableName);
        return this;
    }

    public SelectSqlBean leftJoin(String tableName, String alias) {
        this.table.append(LEFT_JOIN).append(tableName).append(BLANK).append(alias);
        return this;
    }

    public SelectSqlBean join(String tableName) {
        this.table.append(JOIN).append(tableName);
        return this;
    }

    public SelectSqlBean join(String tableName, String alias) {
        this.table.append(JOIN).append(tableName).append(BLANK).append(alias);
        return this;
    }

    public SelectSqlBean rightJoin(String tableName) {
        this.table.append(RIGHT_JOIN).append(tableName);
        return this;
    }

    public SelectSqlBean rightJoin(String tableName, String alias) {
        this.table.append(RIGHT_JOIN).append(tableName).append(BLANK).append(alias);
        return this;
    }

    public SelectSqlBean joinCondition(String str) {
        if (this.joinCondition.length() == 0)
            this.joinCondition.append(ON).append(str);
        else
            this.joinCondition.append(AND).append(str);
        return this;
    }

    public SelectSqlBean joinCondition(String str, Object value) {
        if (this.joinCondition.length() == 0)
            this.joinCondition.append(ON).append(str).append("=").append(value);
        else
            this.joinCondition.append(AND).append(str).append("=").append(value);
        return this;
    }

    public SelectSqlBean groupBy(String column) {
        if (groupByCol.length() == 0) {
            this.groupByCol.append(GROUP_BY).append(column);
        } else {
            this.groupByCol.append(",").append(column);
        }
        return this;
    }

    public SelectSqlBean orderBy(String column) {
        if (orderByCol.length() == 0) {
            this.orderByCol.append(ORDER_BY).append(column);
        } else {
            this.orderByCol.append(",").append(column);
        }
        return this;
    }

    public SelectSqlBean orderBy(String column, String orderType) {
        if (orderByCol.length() == 0) {
            this.orderByCol.append(ORDER_BY).append(column).append(" ").append(orderType);
        } else {
            this.orderByCol.append(",").append(column).append(" ").append(orderType);
        }
        return this;
    }

    public SelectSqlBean addOther(String other) {
        others.append(BLANK).append(other);
        return this;
    }

    public String toString() {
        StringBuilder temp = new StringBuilder().append(SELECT).append(columns.length() > 0 ? columns : "*").append(FROM).append(table)
                .append(joinCondition).append(condition);
        if (orCondition.length() > 0) {
            if (condition.length() == 0)
                temp.append(WHERE);
            temp.append(AND).append(" ( ").append(orCondition).append(" ) ");
        }
        if (groupByCol.length() > 0) {
            temp.append(groupByCol);
        }
        if (orderByCol.length() > 0) {
            temp.append(orderByCol);
        }
        if (others.length() > 0) {
            temp.append(others);
        }
        return temp.toString();
    }

    public String toCountString() {
        StringBuilder temp = new StringBuilder().append(SELECT).append(" count(*) as COUNT ").append(FROM)
                .append(table).append(joinCondition).append(condition);
        if (orCondition.length() > 0) {
            if (condition.length() == 0)
                temp.append(WHERE);
            temp.append(AND).append(" ( ").append(orCondition).append(" ) ");
        }
        if (groupByCol.length() > 0) {
            temp.append(groupByCol);
        }
        if (orderByCol.length() > 0) {
            temp.append(orderByCol);
        }
        if (others.length() > 0) {
            temp.append(others);
        }
        return temp.toString();
    }

    public boolean isPreparedFlag() {
        return preparedFlag;
    }

    public void setPreparedFlag(boolean preparedFlag) {
        this.preparedFlag = preparedFlag;
    }

    public StringBuilder getColumns() {
        return columns;
    }

    public StringBuilder getTable() {
        return table;
    }

    public String page(int offset, int limit) {
        String strSql = toString();
        StringBuffer pagingSelect = new StringBuffer(strSql.length() + 100);
        pagingSelect.append("select * from ( select row_.*, rownum rownum_ from ( ");
        pagingSelect.append(strSql);
        pagingSelect.append(" ) row_ ) where rownum_ > ").append(offset).append(" and rownum_ <= ")
                .append(offset + limit);

        return pagingSelect.toString();
    }

    public String newPage(int offset, int limit) {
        String strSql = toString();
        StringBuffer pagingSelect = new StringBuffer(strSql.length() + 100);
        pagingSelect.append(strSql).append(" limit " + limit + " offset " + offset);
        return pagingSelect.toString();
    }
}
