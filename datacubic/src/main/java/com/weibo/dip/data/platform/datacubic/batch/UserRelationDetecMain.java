package com.weibo.dip.data.platform.datacubic.batch;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.batch.udf.FilterOrefUid;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by xiaoyu on 2017/4/18.
 */
public class UserRelationDetecMain {

    private static Logger LOGGER = LoggerFactory.getLogger(UserRelationDetecMain.class);

    private static String ROOT_PATH = "/tmp/user_relation/";
    private static String MID_PATH = ROOT_PATH + "mid_data_path";
    private static String OREF_UID_PATH = ROOT_PATH + "oref_uid_path";
    private static final String HDFS_OUTPUT_PATH = ROOT_PATH + "result";
    private static String SRC_PATH = "/user/hdfs/rawlog/app_weibomobilekafka1234_openapiop";

    private static final String ES_SERVERS = "first.kafka.dip.weibo.com:9092,second.kafka.dip.weibo.com:9092,third.kafka.dip.weibo.com:9092,fourth.kafka.dip.weibo.com:9092,fifth.kafka.dip.weibo.com:9092";
    private static final String ES_TOPIC = "dip-kafka2es-common";
    private static final String ES_TYPE_OREF = "oref";
    private static final String ES_TYPE_UID = "uid";
    private static final String ES_TYPE_TOUID = "touid";

    private static final String SQL_OREF_PATH = "user_relation_oref.sql";
    private static final String SQL_UID_PATH = "user_relation_uid.sql";
    private static final String SQL_TOUID_PATH = "user_relation_touid.sql";

    private static Pattern PATTERN = Pattern.compile("^^\\[(\\S+)] ([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)$");
    private static Pattern KEY_PATTERN = Pattern.compile("([^,|=|>]+)=>");

    private static int SRC_COL_LEN = 11;
    private static String UACTION_14000008 = "14000008";
    private static String UACTION_14000009 = "14000009";

    private static final String UID = "uid";
    private static final String UACTION = "uaction";
    private static final String OREF = "oref";
    private static final String TOUID = "touid";
    private static final String UIDS = "uids";
    private static String[] COLUMNS = new String[]{UID, UACTION, OREF, TOUID};

    public static String getYesterDay(Date date) {
        SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        cal.add(Calendar.DAY_OF_MONTH, -1);

        return YYYY_MM_DD.format(cal.getTime());
    }

    private static String getInputPath(String yesterday) {
        return SRC_PATH + "/" + yesterday + "/*";
    }

    private static String getMidDataPath(String yesterday) {
        return MID_PATH + "/" + yesterday + "/";
    }

    private static String getOrefUidPath(String yesterday) {
        return OREF_UID_PATH + "/" + yesterday + "/result";
    }

    private static void saveMidData(String inputPath, String midDataPath, JavaSparkContext sparkContext) throws IOException {
        JavaRDD<String> sourceRDD = sparkContext.textFile(inputPath);

        JavaRDD<String> midRDD = sourceRDD.map(line -> {
            Matcher matcher = PATTERN.matcher(line);
            if (matcher.matches()) {
                int groups = matcher.groupCount();
                if (groups == SRC_COL_LEN && (matcher.group(5).equals(UACTION_14000008) || matcher.group(5).equals(UACTION_14000009))) {
                    return line;
                }
            }
            return "";
        }).filter(StringUtils::isNotEmpty);

        Path outputPath = new Path(midDataPath);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            LOGGER.info("midDataPath " + midDataPath + " already exists, deleted");
        }

        midRDD.saveAsTextFile(midDataPath);
    }

    private static Map<String, String> parseExtend(String line) {
        Map<String, String> result = new HashMap<>();

        if (StringUtils.isEmpty(line)) {
            return result;
        }

        List<String> keys = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();

        Matcher matcher = KEY_PATTERN.matcher(line);

        while (matcher.find()) {
            keys.add(matcher.group(1));
            indices.add(matcher.start());
        }

        for (int index = 0; index < keys.size(); index++) {
            String key = keys.get(index);
            String value;

            int keyStart = indices.get(index);

            int valueStart = keyStart + key.length() + 2;
            int valueEnd = line.length();

            if (index < (keys.size() - 1)) {
                valueEnd = indices.get(index + 1) - 1;
            }

            value = line.substring(valueStart, valueEnd);

            result.put(key, value);
        }

        return result;
    }

    private static JavaRDD<Row> midDataToRDD(String midPath, JavaSparkContext sparkContext) {
        JavaRDD<String> sourceRDD = sparkContext.textFile(midPath);

        JavaRDD<Row> rowRDD = sourceRDD.flatMap(line -> {
            List<Row> rows = new ArrayList<>();

            if (StringUtils.isEmpty(line)) {
                return rows.iterator();
            }

            Matcher matcher = PATTERN.matcher(line);

            if (matcher.matches()) {
                int groups = matcher.groupCount();

                if (groups == SRC_COL_LEN &&
                        (matcher.group(5).equals(UACTION_14000008) ||
                                matcher.group(5).equals(UACTION_14000009))) {
                    String uid = matcher.group(4);
                    String uaction = matcher.group(5);
                    String oref = matcher.group(6);
                    String extend = matcher.group(11);
                    String uids = parseExtend(extend).get(UIDS);
                    String[] touids = StringUtils.isNotEmpty(uids) ? uids.split(";") : null;

                    if (StringUtils.isEmpty(uid) || StringUtils.isEmpty(uaction) || StringUtils.isEmpty(oref) || StringUtils.isEmpty(uids) || ArrayUtils.isEmpty(touids)) {
                        return rows.iterator();
                    }

                    for (String touid : touids) {
                        rows.add(RowFactory.create(uid, uaction, oref, touid));
                    }
                }
            } else {
                LOGGER.debug("Parse line " + line + " with regex error: not match");
            }

            return rows.iterator();
        });

        return rowRDD;
    }

    private static StructType createSchema() {
        List<StructField> fields = new ArrayList<>();

        for (String columnName : COLUMNS) {
            fields.add(DataTypes.createStructField(columnName, DataTypes.StringType, true));
        }

        return DataTypes.createStructType(fields);
    }

    private static String getSql(String sqlPath) throws Exception {
        return String.join("\n", IOUtils.readLines(UserRelationDetecMain.class.getClassLoader().getResourceAsStream(sqlPath), CharEncoding.UTF_8));
    }

    private static String getSql(String sqlPath, Iterable<? extends CharSequence> elements) throws Exception {
        return getSql(sqlPath).replace("?", String.join(",", elements));
    }

    private static List<String> getOrefList(List<Row> rowOrefCount) {
        List<String> orefs = new ArrayList<>();
        for (Row row : rowOrefCount) {
            orefs.add(row.getString(0));
        }
        return orefs;
    }

    private static String getOrefSql(String sqlPath, List<Row> rowOrefCount) throws Exception {
        List<String> orefs = getOrefList(rowOrefCount);
        return getSql(sqlPath, orefs);
    }

    private static void saveOrefUidToHdfs(String orefUidPath, List<Row> rows) throws IOException {
        Path outputPath = new Path(orefUidPath);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            LOGGER.info("oref_uid_path " + outputPath + " already exists, deleted");
        }

        BufferedWriter writer = null;

        try {
            writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath), CharEncoding.UTF_8));

            for (Row row : rows) {
                writer.write(row.getString(0).trim() + "," + row.getString(1).trim());
                writer.newLine();
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    private static void saveToEs(List<Row> result, String servers, String topic, String type, String time, String timestamp) {
        if (CollectionUtils.isEmpty(result)) {
            LOGGER.info("results size = 0,return.");

            return;
        }

        Producer<String, String> producer = null;

        try {

            Map<String, Object> config = new HashMap<>();

            config.put("bootstrap.servers", servers);
            config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            producer = new KafkaProducer<>(config);

            for (Row row : result) {
                String[] fieldNames = row.schema().fieldNames();
                Map<String, Object> values = new HashMap<>();

                values.put("index", "dip-user-relation-" + time);
                values.put("type", type);
                values.put("timestamp", timestamp);

                for (int index = 0; index < fieldNames.length; index++) {
                    Object value = row.get(index);
                    values.put(fieldNames[index], value);
                }

                String resultJson = null;

                try {
                    resultJson = GsonUtil.toJson(values);
                } catch (Exception e) {
                    LOGGER.warn("values: " + values + " to json error: " + ExceptionUtils.getFullStackTrace(e));
                    continue;
                }

                if (StringUtils.isNotEmpty(resultJson)) {
                    producer.send(new ProducerRecord<>(topic, resultJson));
                }
            }
        } finally {
            if (producer != null) {
                producer.close();
            }
        }
    }

    private static int saveToEs(Iterator<Row> result, String servers, String topic, String type, String time, String timestamp) {
        if (!result.hasNext()) {
            LOGGER.info("results size = 0,return.");

            return 0;
        }

        Producer<String, String> producer = null;
        int sendNum = 0;

        try {
            Map<String, Object> config = new HashMap<>();

            config.put("bootstrap.servers", servers);
            config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            producer = new KafkaProducer<>(config);

            while (result.hasNext()) {
                Row row = result.next();
                String[] fieldNames = row.schema().fieldNames();
                Map<String, Object> values = new HashMap<>();

                values.put("index", "dip-user-relation-" + time);
                values.put("type", type);
                values.put("timestamp", timestamp);

                for (int index = 0; index < fieldNames.length; index++) {
                    Object value = row.get(index);
                    values.put(fieldNames[index], value);
                }

                String resultJson = null;

                try {
                    resultJson = GsonUtil.toJson(values);
                } catch (Exception e) {
                    LOGGER.warn("values: " + values + " to json error: " + ExceptionUtils.getFullStackTrace(e));
                    continue;
                }

                if (StringUtils.isNotEmpty(resultJson)) {
                    producer.send(new ProducerRecord<>(topic, resultJson));
                    sendNum++;
                }
            }
        } finally {
            if (producer != null) {
                producer.close();
            }
        }

        return sendNum;
    }

    private static void saveToHdfs(List<Row> rows, String outputPathStr) throws Exception {
        int size = CollectionUtils.isEmpty(rows) ? 0 : rows.size();
        LOGGER.info("size: " + size);

        Path outputPath = new Path(outputPathStr);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            LOGGER.info("outputPath " + outputPath + " already exists, deleted");
        }

        LOGGER.info("write data to " + outputPathStr);

        BufferedWriter writer = null;

        try {
            writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath), CharEncoding.UTF_8));

            if (size == 0) {
                writer.write("result size = 0");
                writer.close();

                return;
            }

            String[] fieldNames = rows.get(0).schema().fieldNames();
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < size; i++) {
                Row row = rows.get(i);
                for (int j = 0; j < fieldNames.length; j++) {
                    sb.append(row.get(j).toString() + " ");
                }
                writer.write(sb.toString());
                writer.newLine();
                sb.setLength(0);
            }

        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JavaSparkContext sparkContext = null;

        try {
            Date today;
            SimpleDateFormat YYYYMMDD = new SimpleDateFormat("yyyyMMdd");

            if (ArrayUtils.isEmpty(args)) {
                today = new Date();
            } else {
                if (args.length != 1) {
                    LOGGER.error("input time param len = 1!");
                    return;
                }
                String time = args[0];
                Matcher matcher = Pattern.compile("^\\d{8}$").matcher(args[0]);
                if (!matcher.matches()) {
                    LOGGER.error("input time digital like YYYYMMDD!");
                    return;
                }
                today = YYYYMMDD.parse(time);
            }

            String todaytime = YYYYMMDD.format(today);

            String yesterdaytime = getYesterDay(today);

            String inputPath = getInputPath(yesterdaytime);

            LOGGER.info("inputPath: " + inputPath);

            String midPath = getMidDataPath(yesterdaytime);//

            LOGGER.info("midPath: " + midPath);

            sparkContext = new JavaSparkContext(new SparkConf());

            saveMidData(inputPath, midPath, sparkContext);

            SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext.sc()).enableHiveSupport().getOrCreate();

            String TABLE_NAME = "openapi_op";

            sparkSession.createDataFrame(midDataToRDD(midPath, sparkContext), createSchema()).createOrReplaceTempView(TABLE_NAME);

            String sqlOrefCount = getSql(SQL_OREF_PATH);

            LOGGER.info(sqlOrefCount);
            Dataset<Row> resOrefCount = sparkSession.sql(sqlOrefCount);

            List<Row> rowOrefCount = resOrefCount.javaRDD().collect();

            if (CollectionUtils.isEmpty(rowOrefCount)) {
                LOGGER.info("oref size = 0");

                return;
            }

            SimpleDateFormat utcDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'00:00:00.000'Z'");

            utcDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            String timestamp = utcDateFormat.format(today);

            LOGGER.info(String.format("oref size: %d, begin to save oref to es.", rowOrefCount.size()));
            resOrefCount.foreachPartition(it -> {
                saveToEs(it, ES_SERVERS, ES_TOPIC, ES_TYPE_OREF, todaytime, timestamp);
            });

            String sqlUidcount = getOrefSql(SQL_UID_PATH, rowOrefCount);

            LOGGER.info(sqlUidcount);
            Dataset<Row> resUidCount = sparkSession.sql(sqlUidcount);

            List<Row> rowUidcount = resUidCount.javaRDD().collect();

            if (CollectionUtils.isEmpty(rowUidcount)) {
                LOGGER.info("uid size = 0");

                return;
            }

            LOGGER.info(String.format("uid size: %d, begin to save uid to es.", rowUidcount.size()));
            resUidCount.foreachPartition(it -> {
                saveToEs(it, ES_SERVERS, ES_TOPIC, ES_TYPE_UID, todaytime, timestamp);
            });

            String orefUidPath = getOrefUidPath(yesterdaytime);

            LOGGER.info("orefUidPath path : " + orefUidPath);
            saveOrefUidToHdfs(orefUidPath, rowUidcount);

            sparkSession.udf().register("filterOrefUid", new FilterOrefUid(orefUidPath), DataTypes.StringType);

            String sqlTouidCount = getSql(SQL_TOUID_PATH);
            LOGGER.info(sqlTouidCount);

            Dataset<Row> resTouidCount = sparkSession.sql(sqlTouidCount);

            LOGGER.info("begin to save touid to es.");
            LongAccumulator accumTouid = sparkContext.sc().longAccumulator();

            resTouidCount.foreachPartition(it -> {
                accumTouid.add(saveToEs(it, ES_SERVERS, ES_TOPIC, ES_TYPE_TOUID, todaytime, timestamp));
            });
            LOGGER.info("save touid to es finished. touid size : " + accumTouid.value());
        } finally {
            if (sparkContext != null) {
                sparkContext.close();
                LOGGER.info("sparkContext close.");
            }
        }

        LOGGER.info("all work done.");
    }
}