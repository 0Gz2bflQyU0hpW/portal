package com.weibo.dip.data.platform.datacubic.test;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * Created by yurun on 17/4/19.
 */
public class SparkSQLRowMain {

    public static void main(String[] args) {
        String value1 = "1";
        String value2 = "2";
        String value3 = "3";

        Row row = RowFactory.create(value1, value2, value3);

        System.out.println(row.get(0) == value1);


    }

}
