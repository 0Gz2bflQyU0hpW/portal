package com.weibo.dip.data.platform.datacubic.druid.query.builder;

import com.weibo.dip.data.platform.datacubic.druid.datasource.DataSource;
import com.weibo.dip.data.platform.datacubic.druid.query.Query;

/**
 * Created by yurun on 17/1/24.
 */
public abstract class QueryBuilder<Q extends Query, B extends QueryBuilder> {

    protected Q query;

    public QueryBuilder(Q query) {
        this.query = query;
    }

    public B setQueryType(String queryType) {
        query.setQueryType(queryType);

        return (B) this;
    }

    public B setDataSource(DataSource dataSource) {
        query.setDataSource(dataSource);

        return (B) this;
    }

    public Q build() {
        return query;
    }

}
