package com.weibo.dip.data.platform.datacubic.streaming.mapper;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by yurun on 17/2/23.
 */
public class TextRowMapper implements RowMapper {

    @Override
    public String map(Row row) {
        if (Objects.isNull(row)) {
            return null;
        }

        String[] fieldNames = row.schema().fieldNames();

        StringBuffer sb = new StringBuffer();

        for (int index = 0; index < fieldNames.length; index++) {
            sb.append(row.get(index) + " ");
        }

        sb.append("\n");

        return sb.toString();
    }

    public static void main(String[] args) {
        StringBuffer sb = new StringBuffer();
        Object[] array = {1,"ww",34};

        for (int index = 0; index < array.length; index++) {
            sb.append(array[index] + " ");
        }

        sb.append("\n");
        System.out.println(sb.toString());
    }

}
