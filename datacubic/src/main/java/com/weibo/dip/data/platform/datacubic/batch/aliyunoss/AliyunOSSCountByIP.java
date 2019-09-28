package com.weibo.dip.data.platform.datacubic.batch.aliyunoss;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.batch.aliyunossconfig.BatchSinkStr;
import com.weibo.dip.data.platform.datacubic.batch.aliyunossconfig.BatchSourceStr;
import com.weibo.dip.data.platform.datacubic.batch.aliyunossconfig.BatchSqlStr;
import com.weibo.dip.data.platform.datacubic.batch.batchpojo.BatchConstant;
import com.weibo.dip.data.platform.datacubic.batch.batchpojo.BatchSink;
import com.weibo.dip.data.platform.datacubic.batch.batchpojo.BatchSource;
import com.weibo.dip.data.platform.datacubic.batch.util.BatchUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by xiaoyu on 2017/6/8.
 */

public class AliyunOSSCountByIP {
    private static Logger LOGGER = LoggerFactory.getLogger(AliyunOSSCountByIP.class);

    private static String ROOT_PATH = "/user/hdfs/rawlog/";

    private static BatchSource getBatchSource(){
        String inputPathRoot = ROOT_PATH + BatchSourceStr.DATA_SET;
        String tableName = BatchSourceStr.tableName;
        String format = BatchSourceStr.format;
        String[] columns = BatchSourceStr.columns;

        String regex = format.equals(BatchConstant.SOURCE_INPUT_FORMAT_JSON)? "" : BatchSourceStr.regex;
        return new BatchSource(inputPathRoot, tableName, format, columns, regex);
    }

    private static BatchSink getBatchSink(){
        String server = BatchSinkStr.KAFKA_SERVER;
        String topic  = BatchSinkStr.KAFKA_TOPIC;

        return new BatchSink(server,topic);
    }

    private static String getSql(){
        return BatchSqlStr.sqlIp;
    }

    public static String getYesterDay(Date date) {
        SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        cal.add(Calendar.DAY_OF_MONTH, -1);

        return YYYY_MM_DD.format(cal.getTime());
    }

    private static String getInputPath(String inputPathRoot, Date date) {
        String yesterday = getYesterDay(date);
        return inputPathRoot + "/" + yesterday + "/*";
    }

    public static Date getToday(String[] args) throws Exception{
        if (ArrayUtils.isEmpty(args)) {
            return new Date();
        } else {
            if (args.length != 1) {
                LOGGER.error("input time param len = 1!");
                return null;
            }

            String time = args[0];

            Matcher matcher = Pattern.compile("^\\d{8}$").matcher(time);

            if (!matcher.matches()) {
                LOGGER.error("input time digital like YYYYMMDD!");
                return null;
            }
            return new SimpleDateFormat("yyyyMMdd").parse(time);
        }
    }

    private static Producer<String, String> getProducer(String servers){
        Map<String, Object> config = new HashMap<>();

        config.put("bootstrap.servers", servers);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(config);
    }

    public static void main(String[] args) throws Exception{
        JavaSparkContext sparkContext = null;

        try {
            Date today = getToday(args);

            BatchSource batchSource = getBatchSource();

            BatchSink batchSink = getBatchSink();

            String inputPath = getInputPath(batchSource.getInputPathRoot(),today);

            LOGGER.info("basic: inputpath: " + inputPath + " " +  batchSource.toString() + " " + batchSink.toString());

            sparkContext = new JavaSparkContext(new SparkConf());

            JavaRDD<String> rdd = sparkContext.textFile(inputPath);

            LongAccumulator sourceCount = sparkContext.sc().longAccumulator();
            LongAccumulator processCount = sparkContext.sc().longAccumulator();
            LongAccumulator sinkCount = sparkContext.sc().longAccumulator();

            String[] columns = batchSource.getColumns();

            JavaRDD<Row> rowRDD = rdd.map(line ->{
                sourceCount.add(1L);
                String[] values = null;
                Pattern regexPattern = Pattern.compile(batchSource.getRegex());

                switch (batchSource.getFormat()) {
                    case BatchConstant.SOURCE_INPUT_FORMAT_JSON:
                        Map<String, Object> pairs = null;

                        try {
                            pairs = GsonUtil.fromJson(line, GsonUtil.GsonType.OBJECT_MAP_TYPE);
                        } catch (Exception e) {
                            LOGGER.debug("Parse line " + line + " with json error: " + ExceptionUtils.getFullStackTrace(e));
                        }

                        if (MapUtils.isNotEmpty(pairs)) {
                            values = new String[columns.length];

                            for (int index = 0; index < columns.length; index++) {
                                Object value = pairs.get(columns[index]);

                                values[index] = (value != null) ? String.valueOf(value) : null;
                            }
                        }

                        break;

                    case BatchConstant.SOURCE_INPUT_FORMAT_DELIMITER:
                        String delimiter = batchSource.getRegex();

                        values = line.split(delimiter, -1);

                        if (columns.length != values.length) {
                            values = null;

                            LOGGER.debug("Parse line " + line + " with delimiter error: inconsistent columns");
                        }

                        break;

                    case BatchConstant.SOURCE_INPUT_FORMAT_REGEX:
                        Matcher matcher = regexPattern.matcher(line);

                        if (matcher.matches()) {
                            int groups = matcher.groupCount();

                            if (groups == columns.length) {
                                values = new String[groups];

                                for (int index = 0; index < groups; index++) {
                                    values[index] = matcher.group(index + 1);
                                }
                            } else {
                                LOGGER.debug("Parse line " + line + " with regex error: inconsistent groups");
                            }
                        } else {
                            LOGGER.debug("Parse line " + line + " with regex error: regular expression mismatch");
                        }

                        break;
                }
                return values != null ? RowFactory.create((Object[]) values) : null;

            }).filter(row -> {
                boolean isNonNull = Objects.nonNull(row);
                if (isNonNull) {
                    processCount.add(1L);
                }

                return isNonNull;
            });

            SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext.sc()).enableHiveSupport().getOrCreate();
            String sql = getSql();

            sparkSession.createDataFrame(rowRDD, batchSource.getSchema()).createOrReplaceTempView(batchSource.getTableName());

            Dataset<Row> results = sparkSession.sql(sql);

            if (CollectionUtils.isEmpty(results.javaRDD().collect())){
                LOGGER.info("result size = 0,return.");
                return;
            }

            String path = "/user/tmp/aliyunOssCountByIp";

            BatchUtils.writeToHdfs(results.javaRDD().collect(),path);

            LOGGER.info("RDD source[" + sourceCount.value() + "], process[" + processCount.value() + "], sink[" + sinkCount.value() + "]");

        }finally {
            if (sparkContext != null){
                sparkContext.close();
            }
        }

    }

}
