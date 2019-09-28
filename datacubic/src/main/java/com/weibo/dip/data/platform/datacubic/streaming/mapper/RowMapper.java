package com.weibo.dip.data.platform.datacubic.streaming.mapper;

import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * Created by yurun on 17/2/23.
 */
public interface RowMapper extends Serializable {

    String map(Row row);

}
