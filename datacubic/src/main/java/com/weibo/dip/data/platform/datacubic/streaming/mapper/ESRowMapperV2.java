package com.weibo.dip.data.platform.datacubic.streaming.mapper;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yurun on 17/2/23.
 */
public class ESRowMapperV2 implements RowMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(ESRowMapperV2.class);

    private static final String _INDEX = "_index";
    private static final String _TYPE = "_type";
    private static final String _TIMESTAMP = "_timestamp";

    private static final String INDEX = "index";
    private static final String TYPE = "type";
    private static final String TIMESTAMP = "timestamp";

    private SimpleDateFormat indexDateFormat = new SimpleDateFormat("yyyyMMdd");

    private SimpleDateFormat utcDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    {
        utcDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    public String map(Row row) {
        String[] fieldNames = row.schema().fieldNames();
        StructField[] fieldTypes = row.schema().fields();

        String indexName = row.getString(row.fieldIndex(_INDEX));

        String timestamp = row.getString(row.fieldIndex(_TIMESTAMP));

        Map<String, Object> values = new HashMap<>();

        for (int index = 0; index < fieldNames.length; index++) {
            String fieldName = fieldNames[index];
            DataType fieldType = fieldTypes[index].dataType();

            if (fieldName.equals(_INDEX)) {
                try {
                    values.put(INDEX, indexName + "-" + indexDateFormat.format(utcDateFormat.parse(timestamp)));
                } catch (ParseException e) {
                    LOGGER.error("Parse utc timestamp error: " + ExceptionUtils.getFullStackTrace(e));

                    return null;
                }
            } else if (fieldName.equals(_TYPE)) {
                values.put(TYPE, row.get(index));
            } else if (fieldName.equals(_TIMESTAMP)) {
                values.put(TIMESTAMP, row.get(index));
            } else {
                Object value;

                if (fieldType instanceof ArrayType) {
                    Seq columnValue = (Seq) row.get(index);

                    if (Objects.isNull(columnValue)) {
                        value = null;
                    } else {
                        Iterator iterator = columnValue.iterator();

                        List<Object> datas = new ArrayList<>();

                        while (iterator.hasNext()) {
                            datas.add(iterator.next());
                        }

                        value = datas;
                    }
                } else {
                    value = row.get(index);
                }

                values.put(fieldName, value);
            }
        }

        String result = null;

        try {
            result = GsonUtil.toJson(values);
        } catch (Exception e) {
            LOGGER.warn("values: " + values + " to json error: " + ExceptionUtils.getFullStackTrace(e));
        }

        return result;
    }

}
