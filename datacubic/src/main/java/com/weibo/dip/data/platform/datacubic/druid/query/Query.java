package com.weibo.dip.data.platform.datacubic.druid.query;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.druid.datasource.DataSource;

/**
 * Created by yurun on 17/1/23.
 */
public abstract class Query {

    protected String queryType;

    protected DataSource dataSource;

    protected Context context = new Context();

    public String getQueryType() {
        return queryType;
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public String getQuery() {
        return toString();
    }

    @Override
    public String toString() {
        return GsonUtil.toJson(this);
    }

}
