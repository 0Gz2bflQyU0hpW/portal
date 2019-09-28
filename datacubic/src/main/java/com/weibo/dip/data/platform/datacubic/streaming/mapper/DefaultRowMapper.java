package com.weibo.dip.data.platform.datacubic.streaming.mapper;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by yurun on 17/2/23.
 */
public class DefaultRowMapper implements RowMapper {

    private Map<String, Object> pairs = new HashMap<>();

    @Override
    public String map(Row row) {
        if (Objects.isNull(row)) {
            return null;
        }

        String[] fieldNames = row.schema().fieldNames();

        for (int index = 0; index < fieldNames.length; index++) {
            pairs.put(fieldNames[index], row.get(index));
        }

        return GsonUtil.toJson(pairs);
    }

}
