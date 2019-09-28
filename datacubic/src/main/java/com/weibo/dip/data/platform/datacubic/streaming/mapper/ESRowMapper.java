package com.weibo.dip.data.platform.datacubic.streaming.mapper;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by yurun on 17/2/23.
 */
public class ESRowMapper implements RowMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(ESRowMapper.class);

    private static final String _INDEX = "_index";

    private static final String _TYPE = "_type";

    private static final String TIMESTAMP = "timestamp";

    private SimpleDateFormat indexDateFormat = new SimpleDateFormat("yyyyMMdd");

    private SimpleDateFormat utcDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    {
        utcDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    public String map(Row row) {
        String[] fieldNames = row.schema().fieldNames();

        String indexName = row.getString(row.fieldIndex(_INDEX));

        String typeName = row.getString(row.fieldIndex(_TYPE));

        String timestamp = row.getString(row.fieldIndex(TIMESTAMP));

        Map<String, Object> values = new HashMap<>();

        for (int index = 0; index < fieldNames.length; index++) {
            String fieldName = fieldNames[index];

            if (fieldName.equals(_INDEX) || fieldName.equals(_TYPE)) {
                continue;
            }

            values.put(fieldName, row.get(index));
        }

        Map<String, String> data = new java.util.HashMap<>();

        try {
            data.put("index", indexName + "-" + indexDateFormat.format(utcDateFormat.parse(timestamp)));
        } catch (ParseException e) {
            LOGGER.error("Parse utc timestamp error: " + ExceptionUtils.getFullStackTrace(e));

            return null;
        }

        data.put("type", typeName);
        data.put("data", GsonUtil.toJson(values));

        return GsonUtil.toJson(data);
    }

}
