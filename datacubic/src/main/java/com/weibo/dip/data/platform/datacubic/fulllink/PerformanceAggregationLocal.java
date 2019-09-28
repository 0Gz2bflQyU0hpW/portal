package com.weibo.dip.data.platform.datacubic.fulllink;

import com.weibo.dip.data.platform.datacubic.streaming.udf.IpToLocation;
import com.weibo.dip.data.platform.datacubic.streaming.udf.TimeToUTCWithInterval;
import com.weibo.dip.data.platform.datacubic.streaming.udf.fulllink.ParseUAInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yurun on 17/5/11.
 */
public class PerformanceAggregationLocal {

    private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceAggregationLocal.class);

    private static List<String> getLog() throws IOException {
        List<String> logs = new ArrayList<>();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(PerformanceAggregationLocal.class.getClassLoader().getResourceAsStream("performance.log"), CharEncoding.UTF_8));

            String line;

            while ((line = reader.readLine()) != null) {
                logs.add(line);
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

        return logs;
    }

    private static String getSQL() throws IOException {
        StringBuilder sql = new StringBuilder();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(PerformanceAggregationLocal.class.getClassLoader().getResourceAsStream("performance_aggregation.sql"), CharEncoding.UTF_8));

            String line;

            while ((line = reader.readLine()) != null) {
                sql.append(line);
                sql.append("\n");
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

        return sql.toString();
    }

    public static void main(String[] args) throws Exception {
        List<String> lines = getLog();

        SparkConf conf = new SparkConf();

        JavaSparkContext context = new JavaSparkContext(conf);

        SparkSession session = SparkSession.builder().enableHiveSupport().getOrCreate();

        session.udf().register("time_to_utc_with_interval", new TimeToUTCWithInterval(), DataTypes.StringType);
        session.udf().register("parseUAInfo", new ParseUAInfo(), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
        session.udf().register("ipToLocation", new IpToLocation(), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

        JavaRDD<String> sourceRDD = context.parallelize(lines);

        Dataset<Row> sourceDS = session.read().json(sourceRDD);

        sourceDS.createOrReplaceTempView("source_table");

        Dataset<Row> resultDS = session.sql(getSQL());

        LongAccumulator outputCount = context.sc().longAccumulator("outputCount");

        List<Row> rows = resultDS.javaRDD().collect();

        context.stop();

        if (CollectionUtils.isNotEmpty(rows)) {
            for (Row row : rows) {
                System.out.println(row);
            }
        }
    }

}
