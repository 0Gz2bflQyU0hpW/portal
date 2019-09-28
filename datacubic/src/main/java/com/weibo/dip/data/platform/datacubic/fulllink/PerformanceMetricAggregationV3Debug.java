package com.weibo.dip.data.platform.datacubic.fulllink;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.streaming.udf.IpToLocation;
import com.weibo.dip.data.platform.datacubic.streaming.udf.TimeToUTCWithInterval;
import com.weibo.dip.data.platform.datacubic.streaming.udf.fulllink.ParseUAInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yurun on 17/5/11.
 */
public class PerformanceMetricAggregationV3Debug {

    private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceMetricAggregationV3Debug.class);

    private static final SimpleDateFormat YYYYMMDD = new SimpleDateFormat("yyyyMMdd");

    private static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");

    private static String getToday(String[] args) {
        return ArrayUtils.isNotEmpty(args) ? args[0] : YYYYMMDD.format(new Date());
    }

    private static String getInputPath(String day) throws ParseException {
        return "/user/hdfs/rawlog/app_weibomobile03x4ts1kl_clientperformance/" + YYYY_MM_DD.format(YYYYMMDD.parse(day)) + "/*";
    }

    private static String getSQL(String day) throws Exception {
        StringBuilder sql = new StringBuilder();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(PerformanceMetricAggregationV3Debug.class.getClassLoader().getResourceAsStream("performance_metric_aggregation_v3_table_a_debug.sql"), CharEncoding.UTF_8));

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

        return sql.toString().replaceAll("@TIMESTAMP", String.valueOf(YYYYMMDD.parse(day).getTime()));
    }

    public static void main(String[] args) throws Exception {
        String day = getToday(args);

        LOGGER.info("day: " + day);

        String inputPath = getInputPath(day);

        LOGGER.info("inputPath: " + inputPath);

        SparkConf conf = new SparkConf();

        conf.set("spark.sql.warehouse.dir", "/tmp/warehouse/" + System.currentTimeMillis());

        JavaSparkContext context = new JavaSparkContext(conf);

        SparkSession session = SparkSession.builder().enableHiveSupport().getOrCreate();

        session.udf().register("time_to_utc_with_interval", new TimeToUTCWithInterval(), DataTypes.StringType);
        session.udf().register("parseUAInfo", new ParseUAInfo(), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
        session.udf().register("ipToLocation", new IpToLocation(), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));

        String[] fieldNames = {"source_line", "subtype", "time", "__date", "ua", "network_type", "ip", "sch", "request_url", "result_code", "during_time", "net_time", "parseTime", "lw", "dl", "sc", "ssc", "sr", "ws", "rh", "rb", "ne"};

        List<StructField> fields = new ArrayList<>();

        for (String fieldName : fieldNames) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<String> sourceRDD = context.textFile(inputPath);

        JavaRDD<Row> rowRDD = sourceRDD.map(line -> {
            Map<String, Object> json;

            try {
                json = GsonUtil.fromJson(line, GsonUtil.GsonType.OBJECT_MAP_TYPE);
                if (MapUtils.isEmpty(json)) {
                    return null;
                }
            } catch (Exception e) {
                LOGGER.debug("line " + line + " to json error: " + ExceptionUtils.getFullStackTrace(e));

                return null;
            }

            String[] values = new String[fieldNames.length];

            values[0] = line;

            for (int index = 1; index < fieldNames.length; index++) {
                Object value = json.get(fieldNames[index]);

                values[index] = (value != null) ? String.valueOf(value) : null;
            }

            return RowFactory.create((Object[]) values);
        }).filter(Objects::nonNull);

        Dataset<Row> sourceDS = session.createDataFrame(rowRDD, schema);

        sourceDS.createOrReplaceTempView("source_table");

        Dataset<Row> resultDS = session.sql(getSQL(day));

        resultDS.printSchema();

        List<Row> rows = resultDS.javaRDD().collect();

        if (CollectionUtils.isNotEmpty(rows)) {
            for (Row row : rows) {
                String[] columns = new String[row.size()];

                for (int index = 0; index < row.size(); index++) {
                    columns[index] = String.valueOf(row.get(index));
                }

                LOGGER.info(String.join(", ", columns));
            }
        }

        context.stop();
    }

}
