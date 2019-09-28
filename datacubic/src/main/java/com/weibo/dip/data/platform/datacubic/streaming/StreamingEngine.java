package com.weibo.dip.data.platform.datacubic.streaming;

import com.google.common.base.Preconditions;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.hive.sql.util.HiveSQLUtil;
import com.weibo.dip.data.platform.datacubic.streaming.mapper.RowMapper;
import com.weibo.dip.data.platform.datacubic.streaming.monitor.StreamingMonitor;
import com.weibo.dip.data.platform.datacubic.streaming.util.KafkaProducerFactory;
import com.weibo.dip.data.platform.datacubic.streaming.util.SparkSQLUDFUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import kafka.serializer.StringDecoder;
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
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Created by yurun on 16/12/27.
 */
public class StreamingEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingEngine.class);

    private static Source getDataSource(File sourceConfig) throws Exception {
        Properties properties = new Properties();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(sourceConfig), CharEncoding.UTF_8));

            properties.load(reader);
        } finally {
            IOUtils.closeQuietly(reader);
        }

        String durationStr = properties.getProperty(Constants.SOURCE_DURATION);
        Preconditions.checkState(StringUtils.isNotEmpty(durationStr), Constants.SOURCE_DURATION + " must be specified in " + sourceConfig.getName());

        long duration = Long.valueOf(durationStr);
        Preconditions.checkState(duration > 0, Constants.SOURCE_DURATION + " must larger than zero");

        String approach = properties.getProperty(Constants.SOURCE_APPROACH, Source.RECEIVER);
        Preconditions.checkState(approach.equals(Source.RECEIVER) || approach.equals(Source.DIRECT), Constants.SOURCE_DURATION + " must be " + Source.RECEIVER + " or " + Source.DIRECT);

        String zkQuorums = null;

        String brokers = null;

        if (approach.equals(Source.RECEIVER)) {
            zkQuorums = properties.getProperty(Constants.SOURCE_KAFKA_ZOOKEEPER_QUORUMS);
            Preconditions.checkState(StringUtils.isNotEmpty(zkQuorums), Constants.SOURCE_KAFKA_ZOOKEEPER_QUORUMS + " must be specified in " + sourceConfig.getName());
        } else if (approach.equals(Source.DIRECT)) {
            brokers = properties.getProperty(Constants.SOURCE_KAFKA_BROKERS);
            Preconditions.checkState(StringUtils.isNotEmpty(brokers), Constants.SOURCE_KAFKA_BROKERS + " must be specified in " + sourceConfig.getName());
        }

        String topic = properties.getProperty(Constants.SOURCE_KAFKA_TOPIC_NAME);
        Preconditions.checkState(StringUtils.isNotEmpty(topic), Constants.SOURCE_KAFKA_TOPIC_NAME + " must be specified in " + sourceConfig.getName());

        boolean trim = Boolean.valueOf(properties.getProperty(Constants.SOURCE_KAFKA_TOPIC_TRIM, "true"));

        String format = properties.getProperty(Constants.SOURCE_KAFKA_TOPIC_FORMAT);
        Preconditions.checkState(StringUtils.isNotEmpty(format) && (format.equals(Constants.SOURCE_KAFKA_TOPIC_FORMAT_JSON) || format.equals(Constants.SOURCE_KAFKA_TOPIC_FORMAT_DELIMITER) || format.equals(Constants.SOURCE_KAFKA_TOPIC_FORMAT_REGEX)),
            Constants.SOURCE_KAFKA_TOPIC_FORMAT + " must in [" + Constants.SOURCE_KAFKA_TOPIC_FORMAT_JSON + ", " + Constants.SOURCE_KAFKA_TOPIC_FORMAT_DELIMITER + ", " + Constants.SOURCE_KAFKA_TOPIC_FORMAT_REGEX + "]");

        String regex = null;

        if (format.equals(Constants.SOURCE_KAFKA_TOPIC_FORMAT_JSON)) {
            regex = "";
        } else if (format.equals(Constants.SOURCE_KAFKA_TOPIC_FORMAT_DELIMITER) || format.equals(Constants.SOURCE_KAFKA_TOPIC_FORMAT_REGEX)) {
            regex = properties.getProperty(Constants.SOURCE_KAFKA_TOPIC_REGEX);
            Preconditions.checkState(StringUtils.isNotEmpty(regex), Constants.SOURCE_KAFKA_TOPIC_REGEX + " must be specified in " + sourceConfig.getName());
        }

        String columnsStr = properties.getProperty(Constants.SOURCE_KAFKA_TOPIC_COLUMNS);
        Preconditions.checkState(StringUtils.isNotEmpty(columnsStr), Constants.SOURCE_KAFKA_TOPIC_COLUMNS + " must be specified in " + sourceConfig.getName());

        String[] columns = columnsStr.split(Constants.COMMA);
        Preconditions.checkState(ArrayUtils.isNotEmpty(columns), Constants.SOURCE_KAFKA_TOPIC_COLUMNS + "'s length must larger than zero");

        String consumerGroup = null;

        int receivers = 0;

        if (approach.equals(Source.RECEIVER)) {
            consumerGroup = properties.getProperty(Constants.SOURCE_KAFKA_TOPIC_CONSUMER_GROUP);
            Preconditions.checkState(StringUtils.isNotEmpty(consumerGroup), Constants.SOURCE_KAFKA_TOPIC_CONSUMER_GROUP + " must be specified in " + sourceConfig.getName());

            String receiversStr = properties.getProperty(Constants.SOURCE_KAFKA_TOPIC_CONSUMER_RECEIVERS);
            Preconditions.checkState(StringUtils.isNotEmpty(receiversStr), Constants.SOURCE_KAFKA_TOPIC_CONSUMER_RECEIVERS + " must be specified in " + sourceConfig.getName());

            receivers = Integer.valueOf(receiversStr);
            Preconditions.checkState(receivers > 0, Constants.SOURCE_KAFKA_TOPIC_CONSUMER_RECEIVERS + " must larger than zero");
        }

        String table = properties.getProperty(Constants.SOURCE_TABLE);
        Preconditions.checkState(StringUtils.isNotEmpty(table), Constants.SOURCE_TABLE + " must be specified in " + sourceConfig.getName());

        properties.clear();

        Source source = null;

        if (approach.equals(Source.RECEIVER)) {
            source = new Source(duration, zkQuorums, topic, trim, format, regex, columns, consumerGroup, receivers, table);
        } else if (approach.equals(Source.DIRECT)) {
            source = new Source(duration, brokers, topic, trim, format, regex, columns, table);
        }

        return source;
    }

    private static Sql getSql(File sqlConfig) throws Exception {
        FileInputStream in = null;

        try {
            in = new FileInputStream(sqlConfig);

            return new Sql(String.join(Constants.NEWLINE, IOUtils.readLines(in, CharEncoding.UTF_8)));
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    private static List<Function> getFunctions(String sql) throws Exception {
        String[] functionNames = HiveSQLUtil.getNoBuiltInFunctionNames(sql); //antlr

        if (ArrayUtils.isEmpty(functionNames)) {
            return null;
        }

        List<Function> functions = new ArrayList<>();

        Map<String, Function> localFunctions = getLocalFunctions();

        for (String functionName : functionNames) {
            Function function;

            if (MapUtils.isNotEmpty(localFunctions)) {
                function = localFunctions.get(functionName);

                if (Objects.isNull(function)) {
                    // TODO: get remote functions
                    function = null;
                }

                Preconditions.checkState(Objects.nonNull(function), "Can't load function: " + functionName);

                functions.add(function);
            }
        }

        return functions;
    }

    private static Map<String, Function> getLocalFunctions() throws Exception {
        Properties properties = new Properties();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(StreamingEngine.class.getClassLoader().getResourceAsStream(Constants.FUNCTIONS_CONFIG), CharEncoding.UTF_8));

            properties.load(reader);
        } finally {
            IOUtils.closeQuietly(reader);
        }

        Map<String, Function> functions = new HashMap<>();

        for (String functionName : properties.stringPropertyNames()) {
            String classImpl = properties.getProperty(functionName);

            functions.put(functionName, new Function(functionName, classImpl));
        }

        properties.clear();

        return functions;
    }

    private static Map<String, RowMapper> getRowMappers() throws Exception {
        Properties properties = new Properties();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(StreamingEngine.class.getClassLoader().getResourceAsStream(Constants.MAPPERS_CONFIG), CharEncoding.UTF_8));

            properties.load(reader);
        } finally {
            IOUtils.closeQuietly(reader);
        }

        Map<String, RowMapper> mappers = new HashMap<>();

        for (String name : properties.stringPropertyNames()) {
            String classImpl = properties.getProperty(name);

            mappers.put(name, (RowMapper) Class.forName(classImpl).newInstance());
        }

        properties.clear();

        return mappers;
    }

    private static Sink getSink(File sinkConfig) throws Exception {
        Properties properties = new Properties();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(sinkConfig), CharEncoding.UTF_8));

            properties.load(reader);
        } finally {
            IOUtils.closeQuietly(reader);
        }

        String servers = properties.getProperty(Constants.SINK_KAFKA_SERVERS);
        Preconditions.checkState(StringUtils.isNotEmpty(servers), Constants.SINK_KAFKA_SERVERS + " must be specified in " + sinkConfig.getName());

        String topic = properties.getProperty(Constants.SINK_KAFKA_TOPIC);
        Preconditions.checkState(StringUtils.isNotEmpty(topic), Constants.SINK_KAFKA_TOPIC + " must be specified in " + sinkConfig.getName());

        String mapperName = properties.getProperty(Constants.SINK_KAFKA_MAPPER);
        Preconditions.checkState(StringUtils.isNotEmpty(mapperName), Constants.SINK_KAFKA_MAPPER + " must be specified in " + sinkConfig.getName());

        RowMapper mapper = getRowMappers().get(mapperName);
        Preconditions.checkState(Objects.nonNull(mapper), "Can't find rowMapper instance for " + mapperName);

        properties.clear();

        return new Sink(servers, topic, mapper);
    }

    public static void main(String[] args) throws Exception {
        Preconditions.checkState(ArrayUtils.isNotEmpty(args) && args.length == 1, "We need specify a conf dir!");

        String confDir = args[0];

        LOGGER.info("conf dir: " + confDir);

        File sourceConfig = new File(confDir, Constants.SOURCE_CONFIG);
        Preconditions.checkState(sourceConfig.exists() && sourceConfig.isFile(), "Can't find " + Constants.SOURCE_CONFIG + " in " + confDir);

        File sqlConfig = new File(confDir, Constants.SQL_CONFIG);
        Preconditions.checkState(sqlConfig.exists() && sqlConfig.isFile(), "Can't find " + Constants.SQL_CONFIG + " in " + confDir);

        File sinkConfig = new File(confDir, Constants.SINK_CONFIG);
        Preconditions.checkState(sinkConfig.exists() && sinkConfig.isFile(), "Can't find " + Constants.SINK_CONFIG + " in " + confDir);

        Source source = getDataSource(sourceConfig);

        Sql sql = getSql(sqlConfig);

        List<Function> functions = getFunctions(sql.getSql());

        Sink sink = getSink(sinkConfig);

        SparkConf sparkConf = new SparkConf();

        if (CollectionUtils.isNotEmpty(functions)) {
            String sparkJars = sparkConf.get(Constants.SPARK_YARN_JARS, Constants.EMPTY_STRING);

            for (Function function : functions) {
                String location = function.getLocation();

                if (location.equals(Function.LOCAL)) {
                    continue;
                }

                if (sparkJars.equals(Constants.EMPTY_STRING)) {
                    sparkJars += location;
                } else {
                    sparkJars += (Constants.COMMA + location);
                }
            }
        }

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        LOGGER.info(source.toString());

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.milliseconds(source.getDuration()));

        SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext.sc()).enableHiveSupport().getOrCreate();

        LOGGER.info(sql.toString());

        if (CollectionUtils.isNotEmpty(functions)) {
            for (Function function : functions) {
                try {
                    SparkSQLUDFUtil.registerUDF(sparkSession.udf(), function.getName(), function.getClassImpl());

                    LOGGER.info("Load function " + function.getName() + "(" + function.getClassImpl() + ")");
                } catch (Exception e) {
                    LOGGER.error("Load function " + function.getName() + "(" + function.getClassImpl() + ") error: " + ExceptionUtils.getFullStackTrace(e));

                    return;
                }
            }
        }

        JavaPairDStream<String, String> kafkaStream;

        if (source.isReceiver()) {
            Map<String, Integer> topics = new HashMap<>();

            topics.put(source.getTopic(), 1);

            int receivers = source.getReceivers();

            List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(receivers);

            for (int index = 0; index < receivers; index++) {
                kafkaStreams.add(KafkaUtils.createStream(streamingContext, source.getZkQuorums(), source.getConsumerGroup(), topics));
            }

            kafkaStream = streamingContext.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));
        } else {
            Map<String, String> kafkaParams = new HashMap<>();

            kafkaParams.put(Constants.BOOTSTRAP_SERVERS, source.getBrokers());

            Set<String> topics = new HashSet<>();

            topics.add(source.getTopic());

            kafkaStream = KafkaUtils.createDirectStream(streamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
        }

        JavaDStream<String> sourceStream = kafkaStream.map(Tuple2::_2).filter(StringUtils::isNotEmpty);

        if (source.isTrim()) {
            sourceStream = sourceStream.map(String::trim);
        } else {
            sourceStream = sourceStream.map(line -> {
                if (line.charAt(line.length() - 1) == '\n') {
                    line = line.substring(0, line.length() - 1);
                }

                return line;
            });
        }

        sourceStream.foreachRDD(sourceRDD -> {
            LongAccumulator sourceCount = sparkContext.sc().longAccumulator();
            LongAccumulator processCount = sparkContext.sc().longAccumulator();
            LongAccumulator sinkCount = sparkContext.sc().longAccumulator();

            String[] columns = source.getColumns();

            Pattern regexPattern = Pattern.compile(source.getRegex());

            JavaRDD<Row> rowRDD = sourceRDD.map(line -> {
                sourceCount.add(1L);

                String[] values = null;

                switch (source.getFormat()) {
                    case Constants.SOURCE_KAFKA_TOPIC_FORMAT_JSON:
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

                    case Constants.SOURCE_KAFKA_TOPIC_FORMAT_DELIMITER:
                        String delimiter = source.getRegex();

                        values = line.split(delimiter, -1);

                        if (columns.length != values.length) {
                            values = null;

                            LOGGER.debug("Parse line " + line + " with delimiter error: inconsistent columns");
                        }

                        break;

                    case Constants.SOURCE_KAFKA_TOPIC_FORMAT_REGEX:
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

            sparkSession.createDataFrame(rowRDD, source.getSchema()).createOrReplaceTempView(source.getTable());

            Dataset<Row> results = sparkSession.sql(sql.getSql());

            results.javaRDD().foreachPartition(iterator -> {
                Producer<String, String> producer = KafkaProducerFactory.getProducer(sink.getServers());

                RowMapper mapper = sink.getMapper();

                while (iterator.hasNext()) {
                    Row row = iterator.next();

                    String line = mapper.map(row);

                    if (StringUtils.isNotEmpty(line)) {
                        producer.send(new ProducerRecord<>(sink.getTopic(), line));

                        sinkCount.add(1L);
                    }
                }
            });

            LOGGER.info("RDD source[" + sourceCount.value() + "], process[" + processCount.value() + "], sink[" + sinkCount.value() + "]");
        });

        Runtime.getRuntime().addShutdownHook(new Thread(streamingContext::stop));

        streamingContext.addStreamingListener(new StreamingMonitor(sparkConf.get("spark.app.name")));

        streamingContext.start();

        streamingContext.awaitTermination();
    }

}
