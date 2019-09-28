package com.weibo.dip.data.platform.datacubic.business.messagekpi.util;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.GsonUtil.GsonType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liyang28 on 2018/7/17.
 */
public class MessageKpiCommon {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageKpiCommon.class);
  private static final FastDateFormat YYYY_MM_DD = FastDateFormat.getInstance("yyyy_MM_dd");
  private static final String SUMMON_BUSINESS = "business";
  private static final String SUMMON_TIMESTAMP = "timestamp";
  public static final String PARSE_UA_INFO = "parseUAInfo";
  public static final String IP_TO_LOCATION = "ipToLocation";
  public static final String SPARK_CACHE_TABLE = "message_kpi";
  public static final String OUTPUT_COUNT = "outputCount";
  public static final String NOT_MATCH_COUNT = "notMatchCount";

  public static final String V1_TABLE_A = "message_kpi_v1_table_a.sql";
  public static final String V1_TABLE_A_BUSINESS = "fulllink-message-client-a";

  public static final String V1_TABLE_B = "message_kpi_v1_table_b.sql";
  public static final String V1_TABLE_B_BUSINESS = "fulllink-message-client-b";

  public static final String V1_TABLE_C = "message_kpi_v1_table_c.sql";
  public static final String V1_TABLE_C_BUSINESS = "fulllink-message-client-c";

  public static final String V2_TABLE_A = "message_kpi_v2_table_a.sql";
  public static final String V2_TABLE_A_BUSINESS = "fulllink-message-playvideo-a";

  public static final String V2_TABLE_B = "message_kpi_v2_table_b.sql";
  public static final String V2_TABLE_B_BUSINESS = "fulllink-message-playvideo-b";

  public static final String V3_TABLE_A = "message_kpi_v3_table_a.sql";
  public static final String V3_TABLE_A_BUSINESS = "fulllink-message-upload-download-a";

  public static final String V4_TABLE_A = "message_kpi_v4_table_a.sql";
  public static final String V4_TABLE_A_BUSINESS = "fulllink-message-send-a";

  public static final String V6_TABLE_A = "message_kpi_v6_table_a.sql";
  public static final String V6_TABLE_A_BUSINESS = "fulllink-message-platform-http";

  public static final String V7_TABLE_A = "message_kpi_v7_table_a.sql";
  public static final String V7_TABLE_A_BUSINESS = "fulllink-message-platform-tcp";

  public static final String V8_TABLE_A = "message_kpi_v8_table_a.sql";
  public static final String V8_TABLE_A_BUSINESS = "fulllink-message-platform-offline";

  public static final String V9_TABLE_A = "message_kpi_v9_table_a.sql";
  public static final String V9_TABLE_A_BUSINESS = "fulllink-message-platform-online";

  /**
   * 获得 json data rdd .
   *
   * @param sc .
   * @param inputPath .
   * @param fieldNames .
   * @return .
   */
  public static JavaRDD<Row> getJsonDataRdd(JavaSparkContext sc, String inputPath,
      String[] fieldNames, LongAccumulator notMatchCount) {
    return sc.textFile(inputPath).map((String line) -> {
      Map<String, Object> json;
      try {
        String regx = "^\\{.*}$";
        Pattern compile = Pattern.compile(regx);
        Matcher matcher = compile.matcher(line);
        if (!matcher.matches()) {
          return null;
        }
        json = GsonUtil.fromJson(line, GsonType.OBJECT_MAP_TYPE);
        if (MapUtils.isEmpty(json)) {
          return null;
        }
      } catch (Exception e) {
        notMatchCount.add(1);
        LOGGER.error("line {} to json error: {}", line, ExceptionUtils.getFullStackTrace(e));
        return null;
      }
      String[] values = new String[fieldNames.length];
      for (int index = 0; index < fieldNames.length; index++) {
        Object value = json.get(fieldNames[index]);
        values[index] = (value != null) ? String.valueOf(value) : null;
      }
      return RowFactory.create((Object[]) values);
    }).filter(Objects::nonNull);
  }

  /**
   * 平台 http 数据格式获取 .
   *
   * @param sc .
   * @param inputPath .
   * @param fieldNames .
   * @return .
   */
  public static JavaRDD<Row> getNotJsonDataRddHttp(JavaSparkContext sc, String inputPath,
      String[] fieldNames, LongAccumulator notMatchCount) {
    String lineRegx = "([^\\s]*) ([^\\s]*) ([^\\s]*) - \\[(.*)] \"(.*)?\" ([^\\s]*) ([^\\s]*) "
        + "\"(.*)?\" \"(.*)?\" \"(.*)?\" \"(.*)?\" \"(.*)?\" \"(.*)?\" \"(.*)?\" \"(.*)\"";
    return sc.textFile(inputPath).map((String line) -> {
      Map<String, Object> json;
      try {
        Pattern lineCompile = Pattern.compile(lineRegx);
        Matcher matcher1 = lineCompile.matcher(line);
        Map<String, Object> map = new HashMap<>();
        if (matcher1.matches()) {
          String request = matcher1.group(5);
          Long status = Long.valueOf(matcher1.group(6));
          Float requestTime = Float.valueOf(matcher1.group(15));
          map.put("request", request);
          map.put("status", status);
          map.put("requestTime", requestTime);
          line = GsonUtil.toJson(map, GsonType.OBJECT_MAP_TYPE);
        } else {
          return null;
        }

        json = GsonUtil.fromJson(line, GsonType.OBJECT_MAP_TYPE);
        if (MapUtils.isEmpty(json)) {
          return null;
        }
      } catch (Exception e) {
        notMatchCount.add(1);
        LOGGER.error("line {} to http error: {}", line, ExceptionUtils.getFullStackTrace(e));
        return null;
      }

      String[] values = new String[fieldNames.length];

      for (int index = 0; index < fieldNames.length; index++) {
        Object value = json.get(fieldNames[index]);
        values[index] = (value != null) ? String.valueOf(value) : null;
      }

      return RowFactory.create((Object[]) values);

    }).filter(Objects::nonNull);
  }

  /**
   * 平台 tcp 数据格式获取 .
   *
   * @param sc .
   * @param inputPath .
   * @param fieldNames .
   * @return .
   */
  public static JavaRDD<Row> getNotJsonDataRddTcp(JavaSparkContext sc, String inputPath,
      String[] fieldNames, LongAccumulator notMatchCount) {
    String lineRegx = "([^\\s]*) ([^\\s]*) (.*)$";
    return sc.textFile(inputPath).map((String line) -> {
      Map<String, Object> json;
      try {
        Pattern lineCompile = Pattern.compile(lineRegx);
        Matcher matcher1 = lineCompile.matcher(line);
        if (matcher1.matches()) {
          line = matcher1.group(3).trim();
        } else {
          return null;
        }
        json = GsonUtil.fromJson(line, GsonType.OBJECT_MAP_TYPE);
        if (MapUtils.isEmpty(json)) {
          return null;
        }
      } catch (Exception e) {
        notMatchCount.add(1);
        LOGGER.error("line {} to tcp error: {}", line, ExceptionUtils.getFullStackTrace(e));
        return null;
      }
      String[] values = new String[fieldNames.length];
      for (int index = 0; index < fieldNames.length; index++) {
        Object value = json.get(fieldNames[index]);
        values[index] = (value != null) ? String.valueOf(value) : null;
      }
      return RowFactory.create((Object[]) values);
    }).filter(Objects::nonNull);
  }

  /**
   * 获得 schema .
   *
   * @param fieldNames .
   * @return .
   */
  public static StructType getSchema(String[] fieldNames) {
    //new
    List<StructField> fields = new ArrayList<>();
    for (String fieldName : fieldNames) {
      fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
    }
    return DataTypes.createStructType(fields);
  }

  /**
   * 获得kafka的服务配置 .
   *
   * @return .
   */
  public static String getKafkaServers() {
    return "first.kafka.dip.weibo.com:9092,second.kafka.dip.weibo.com:9092,third.kafka.dip.weibo.co"
        + "m:9092,fourth.kafka.dip.weibo.com:9092,fifth.kafka.dip.weibo.com:9092";
  }

  /**
   * 获得kafka的topic .
   *
   * @return .
   */
  private static String getKafkaTopic() {
    return "dip-kafka2es-common";
  }

  /**
   * 获得kafka的config .
   *
   * @return .
   */
  private static Map<String, Object> getKafkaConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrap.servers", getKafkaServers());
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return config;
  }

  /**
   * send kafka .
   *
   * @param resultDs .
   * @param date .
   * @param outputCount .
   */
  public static void sendKafka(Dataset<Row> resultDs, String date,
      LongAccumulator outputCount, String business) {
    resultDs.javaRDD().foreachPartition(iterator -> {
      Producer<String, String> producer = null;
      try {
        producer = new KafkaProducer<>(MessageKpiCommon.getKafkaConfig());
        while (iterator.hasNext()) {
          Row row = iterator.next();
          Map<String, Object> values = new HashMap<>();
          values.put(MessageKpiCommon.SUMMON_BUSINESS, business);
          //昨天的时间戳,00:00:00
          values.put(MessageKpiCommon.SUMMON_TIMESTAMP, YYYY_MM_DD.parse(date).getTime());
          String[] rowFieldNames = row.schema().fieldNames();
          for (int index = 0; index < rowFieldNames.length; index++) {
            String fieldName = rowFieldNames[index];
            Object value = row.get(index);
            values.put(fieldName, value);
          }
          try {
            producer.send(new ProducerRecord<>(MessageKpiCommon.getKafkaTopic(),
                GsonUtil.toJson(values)));
            outputCount.add(1L);
          } catch (Exception e) {
            LOGGER.debug("producer send record error: {}", ExceptionUtils.getFullStackTrace(e));
          }
        }
      } catch (Exception e) {
        LOGGER.error("producer send error: {}", ExceptionUtils.getFullStackTrace(e));
      } finally {
        if (producer != null) {
          producer.close();
        }
      }
    });
  }

}
