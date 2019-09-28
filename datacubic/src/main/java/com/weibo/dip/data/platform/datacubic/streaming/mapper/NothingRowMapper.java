package com.weibo.dip.data.platform.datacubic.streaming.mapper;

import org.apache.spark.sql.Row;

/**
 * Created by yurun on 17/2/23.
 */
public class NothingRowMapper implements RowMapper {

    @Override
    public String map(Row row) {
        return null;
    }

}
