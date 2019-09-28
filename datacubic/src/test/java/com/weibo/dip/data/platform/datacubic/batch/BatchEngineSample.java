package com.weibo.dip.data.platform.datacubic.batch;

import com.google.common.base.Preconditions;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.batch.batchpojo.BatchConstant;
import com.weibo.dip.data.platform.datacubic.batch.batchpojo.BatchSink;
import com.weibo.dip.data.platform.datacubic.batch.batchpojo.BatchSource;
import com.weibo.dip.data.platform.datacubic.batch.udf.getDayUTC;
import com.weibo.dip.data.platform.datacubic.batch.util.BatchUtils;
import com.weibo.dip.data.platform.datacubic.streaming.mapper.ESRowMapperV2;
import com.weibo.dip.data.platform.datacubic.streaming.mapper.RowMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by xiaoyu on 2017/6/8.
 */

public class BatchEngineSample {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchEngineSample.class);

    private static final String ROOT_PATH = "/user/hdfs/rawlog/";

    private static BatchSource getBatchSource(File sourceConfig) throws Exception{
        Properties properties = new Properties();

        properties.load(new BufferedReader(new InputStreamReader(new FileInputStream(sourceConfig), CharEncoding.UTF_8)));

        String dataset = properties.getProperty(BatchConstant.SOURCE_INPUT_DATASET);
        Preconditions.checkState(StringUtils.isNotEmpty(dataset), BatchConstant.SOURCE_INPUT_DATASET + " must be specified in " + sourceConfig.getName());

        String inputPathRoot = ROOT_PATH + dataset;

        String format = properties.getProperty(BatchConstant.SOURCE_INPUT_FORMAT);
        Preconditions.checkState(StringUtils.isNotEmpty(format) && (format.equals(BatchConstant.SOURCE_INPUT_FORMAT_JSON) || format.equals(BatchConstant.SOURCE_INPUT_FORMAT_DELIMITER) || format.equals(BatchConstant.SOURCE_INPUT_FORMAT_REGEX)),
                BatchConstant.SOURCE_INPUT_FORMAT + " must in [" + BatchConstant.SOURCE_INPUT_FORMAT_JSON + ", " + BatchConstant.SOURCE_INPUT_FORMAT_DELIMITER + ", " + BatchConstant.SOURCE_INPUT_FORMAT_REGEX + "]");

        String regex = null;

        if (format.equals(BatchConstant.SOURCE_INPUT_FORMAT_JSON)) {
            regex = "";
        } else if (format.equals(BatchConstant.SOURCE_INPUT_FORMAT_DELIMITER) || format.equals(BatchConstant.SOURCE_INPUT_FORMAT_REGEX)) {
            regex = properties.getProperty(BatchConstant.SOURCE_INPUT_REGEX);
            Preconditions.checkState(StringUtils.isNotEmpty(regex), BatchConstant.SOURCE_INPUT_REGEX + " must be specified in " + sourceConfig.getName());
        }


        String tableName = properties.getProperty(BatchConstant.SOURCE_TABLE);
        Preconditions.checkState(StringUtils.isNotEmpty(tableName), BatchConstant.SOURCE_TABLE + " must be specified in " + sourceConfig.getName());

        String columnsStr = properties.getProperty(BatchConstant.SOURCE_INPUT_COLUMNS);
        Preconditions.checkArgument(StringUtils.isNotEmpty(columnsStr), BatchConstant.SOURCE_INPUT_COLUMNS + " must be specified in " + sourceConfig.getName());

        String[] columns = columnsStr.split(BatchConstant.COMMA);
        Preconditions.checkState(ArrayUtils.isNotEmpty(columns) && columns.length > 0, BatchConstant.SOURCE_INPUT_COLUMNS + "'s length must larger than zero");

        properties.clear();

        return new BatchSource(inputPathRoot, tableName, format, columns, regex);
    }

    private static BatchSink getBatchSink(File sinkConfig) throws Exception{
        Properties properties = new Properties();

        properties.load(new BufferedReader(new InputStreamReader(new FileInputStream(sinkConfig), CharEncoding.UTF_8)));

        String mapperName = properties.getProperty(BatchConstant.SINK_OUTPUT_MAPPER);
        Preconditions.checkState(StringUtils.isNotEmpty(mapperName), BatchConstant.SINK_OUTPUT_MAPPER + " must be specified in " + sinkConfig.getName());

        RowMapper mapper = BatchUtils.getRowMappers().get(mapperName);
        Preconditions.checkState(Objects.nonNull(mapper), "Can't find rowMapper instance for " + mapperName);

        String direction = properties.getProperty(BatchConstant.SINK_OUTPUT_DIRECTION);
        Preconditions.checkArgument(StringUtils.isNotEmpty(direction),BatchConstant.SINK_OUTPUT_DIRECTION + " must be specified in " + sinkConfig.getName());
        
        String hdfsPath = properties.getProperty(BatchConstant.SINK_OUTPUT_HDFS_PATH);
        Preconditions.checkArgument(StringUtils.isNotEmpty(hdfsPath),BatchConstant.SINK_OUTPUT_HDFS_PATH + " must be specified in " + sinkConfig.getName());

        String server = properties.getProperty(BatchConstant.SINK_KAFKA_SERVERS);
        Preconditions.checkState(StringUtils.isNotEmpty(server), BatchConstant.SINK_KAFKA_SERVERS + " must be specified in " + sinkConfig.getName());

        String topic = properties.getProperty(BatchConstant.SINK_KAFKA_TOPIC);
        Preconditions.checkState(StringUtils.isNotEmpty(server), BatchConstant.SINK_KAFKA_TOPIC + " must be specified in " + sinkConfig.getName());
        
        properties.clear();

        return new BatchSink(mapper, direction, hdfsPath, server, topic);
    }

    private static String getSql(File sqlConfig) throws Exception{
        FileInputStream in = null;

        try {
            in = new FileInputStream(sqlConfig);

            return String.join(BatchConstant.NEWLINE, IOUtils.readLines(in, CharEncoding.UTF_8));
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    public static void main(String[] args) throws Exception{

        Preconditions.checkState(ArrayUtils.isNotEmpty(args) && (args.length == 1 || args.length == 2), "We need specify a conf dir [optional date:yyyyMMdd]!");

        File sourceConfig = new File(args[0], BatchConstant.SOURCE_CONFIG);
        Preconditions.checkState(sourceConfig.exists() && sourceConfig.isFile(), "Can't find " + BatchConstant.SOURCE_CONFIG + " in " + args[0]);

        File sqlConfig = new File(args[0], BatchConstant.SQL_CONFIG);
        Preconditions.checkState(sqlConfig.exists() && sqlConfig.isFile(), "Can't find " + BatchConstant.SQL_CONFIG + " in " + args[0]);

        File sinkConfig = new File(args[0], BatchConstant.SINK_CONFIG);
        Preconditions.checkState(sinkConfig.exists() && sinkConfig.isFile(), "Can't find " + BatchConstant.SINK_CONFIG + " in " + args[0]);

        JavaSparkContext sparkContext = null;

        try {
            Date today = BatchUtils.getTodayDate(args);

            BatchSource batchsource = getBatchSource(sourceConfig);

            String inputPath = BatchUtils.getInputPathYesterday(batchsource.getInputPathRoot(),today);

            sparkContext = new JavaSparkContext(new SparkConf());

            JavaRDD<String> rdd = sparkContext.textFile(inputPath);

            LongAccumulator sourceCount = sparkContext.sc().longAccumulator();
            LongAccumulator processCount = sparkContext.sc().longAccumulator();
            LongAccumulator sinkCount = sparkContext.sc().longAccumulator();

            String[] columns = batchsource.getColumns();

            JavaRDD<Row> rowRDD = rdd.map(line ->{
                sourceCount.add(1L);
                String[] values = null;
                Pattern regexPattern = Pattern.compile(batchsource.getRegex());

                switch (batchsource.getFormat()) {
                    case BatchConstant.SOURCE_INPUT_FORMAT_JSON:
                        Map<String, Object> pairs = null;

                        try {
                            pairs = GsonUtil.fromJson(line, GsonUtil.GsonType.OBJECT_MAP_TYPE);
                        } catch (Exception e) {
                            LOGGER.debug("Parse line " + line + " with json error: " + ExceptionUtils.getFullStackTrace(e));
                            return null;
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
                        String delimiter = batchsource.getRegex();

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
            String sql = getSql(sqlConfig);

            {//registerUDF();
                sparkSession.udf().register("getdayUTC",new getDayUTC(),DataTypes.StringType);
            }

            sparkSession.createDataFrame(rowRDD, batchsource.getSchema()).createOrReplaceTempView(batchsource.getTableName());

            Dataset<Row> results = sparkSession.sql(sql);

            if (CollectionUtils.isEmpty(results.javaRDD().collect())){
                LOGGER.info("result size = 0,return.");
                return;
            }

            BatchSink batchSink = getBatchSink(sinkConfig);

            switch (batchSink.getDirection()){
                case BatchConstant.SINK_OUTPUT_DERECTION_ES:{
                    results.foreachPartition(rowIterator -> {
                        Producer<String, String> producer = BatchUtils.getProducer(batchSink.getServers());

                        RowMapper mapper = new ESRowMapperV2();

                        while (rowIterator.hasNext()) {
                            Row row = rowIterator.next();

                            String line = mapper.map(row);

                            if (StringUtils.isNotEmpty(line)) {
                                producer.send(new ProducerRecord<>(batchSink.getTopic(), line));

                                sinkCount.add(1L);
                            }
                        }
                    });
                }
                break;
                case BatchConstant.SINK_OUTPUT_DERECTION_HDFS:{
                    long sendNum = BatchUtils.writeToHdfs(results.javaRDD().collect(),batchSink.getHdfsPath());
                    
                    sinkCount.add(sendNum);
                }
                break;
                default:{
                    String message = String.format("direction : '%s' : invalid sink direction,return.",batchSink.getDirection());
                    LOGGER.info(message);
                    return;
                }
            }

            LOGGER.info("source: " + batchsource.toString());

            LOGGER.info("inputPath: " + inputPath);

            LOGGER.info("sink: " + batchSink.toString());

            LOGGER.info("RDD source[" + sourceCount.value() + "], process[" + processCount.value() + "], sink[" + sinkCount.value() + "]");

        }finally {
            if (sparkContext != null){
                sparkContext.close();
            }
        }

    }

}
