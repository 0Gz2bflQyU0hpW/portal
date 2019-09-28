package com.weibo.dip.data.platform.datacubic.fulllink;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.streaming.core.DipStreaming;
import com.weibo.dip.data.platform.datacubic.streaming.core.KafkaProducerProxy;
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
 * Created by qianqian25 on 2018/01/18.
 */
public class PerformanceVideoTransV1 {
    private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceVideoTransV1.class);

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd/HH");

    private static String getToday(String[] args) {
        return ArrayUtils.isNotEmpty(args) ? args[0] : sdf.format(new Date(new Date().getTime() - 60 * 60 * 1000));
    }

    private static String getInputPath(String date) throws ParseException {
        return "/user/hdfs/rawlog/app_picserversweibof6vwt_weibovideotrans/" + date;
    }

    private static long getTimestamp(String[] args) throws ParseException {
        return sdf.parse(getToday(args)).getTime();
    }

    private static String getSQL(String sqlContext) throws Exception {
        StringBuilder sql = new StringBuilder();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(
                    new InputStreamReader(
                            PerformanceVideoTransV1.class.getClassLoader()
                                    .getResourceAsStream(sqlContext),
                            CharEncoding.UTF_8));

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

        String date = getToday(args);

        LOGGER.info("date: " + date);

        String inputPath = getInputPath(date);

        LOGGER.info("inputPath: " + inputPath);

        SparkConf sparkConf = new SparkConf();

        JavaSparkContext context = new JavaSparkContext(sparkConf);

        SparkSession session = SparkSession.builder().enableHiveSupport().getOrCreate();

        Map<String, Object> config = new HashMap<>();

        config.put("bootstrap.servers", "first.kafka.dip.weibo.com:9092,second.kafka.dip.weibo.com:9092,third.kafka.dip.weibo.com:9092,fourth.kafka.dip.weibo.com:9092,fifth.kafka.dip.weibo.com:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducerProxy producer = new KafkaProducerProxy(sparkConf.get(DipStreaming.SPARK_APP_NAME), config);

        String[] fieldNames = {"object_id",
                "job_create_time",
                "client",
                "fast",
                "label",
                "error_code",
                "prev_vbitrate",
                "prev_width",
                "prev_height",
                "post_vbitrate",
                "file_duration",
                "post_file_bitrate",
                "sinatrans_duration",
                "source_file_duration"};

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

            for (int index = 0; index < fieldNames.length; index++) {
                Object value = json.get(fieldNames[index]);

                values[index] = (value != null) ? String.valueOf(value) : null;
            }

            return RowFactory.create((Object[]) values);
        }).filter(Objects::nonNull);

        Dataset<Row> sourceDS = session.createDataFrame(rowRDD, schema);

        sourceDS.createOrReplaceTempView("tab_summary");

        String[] sqls = {"weibo_video_trans.sql", "weibo_video_trans_input.sql", "weibo_video_trans_kpi.sql"};

        for(String sqlcontext : sqls) {
            Dataset<Row> tabADS = session.sql(getSQL(sqlcontext));

            tabADS.javaRDD().foreachPartition(iterator -> {
                try {

                    while (iterator.hasNext()) {
                        Row row = iterator.next();

                        Map<String, Object> values = new HashMap<>();

                        values.put("timestamp", getTimestamp(args));

                        String[] rowFieldNames = row.schema().fieldNames();

                        for (int index = 0; index < rowFieldNames.length; index++) {
                            String fieldName = rowFieldNames[index];
                            Object value = row.get(index);
                            values.put(fieldName, value);
                        }

                        try {
                            producer.send("dip-kafka2es-common", GsonUtil.toJson(values));

                        } catch (Exception e) {
                            LOGGER.error("<weibo_video_trans> producer send record error: " + ExceptionUtils.getFullStackTrace(e));
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("<weibo_video_trans> producer send error: " + ExceptionUtils.getFullStackTrace(e));
                }
            });
        }
        context.stop();
    }
}
