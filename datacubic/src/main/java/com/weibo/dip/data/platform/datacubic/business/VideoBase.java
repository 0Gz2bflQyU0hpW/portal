package com.weibo.dip.data.platform.datacubic.business;

import com.google.common.util.concurrent.RateLimiter;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VideoBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(VideoBase.class);

  private static final SimpleDateFormat YYYYMMDD = new SimpleDateFormat("yyyyMMdd");

  private static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");

  private static String getToday(String[] args) {
    return ArrayUtils.isNotEmpty(args) ? args[0] : YYYYMMDD.format(new Date());
  }

  private static String getrawPath(String day) throws ParseException {
    return "/user/hdfs/rawlog/app_weibomobilekafka1234_weibomobileaction799/"
      + YYYY_MM_DD.format(YYYYMMDD.parse(day)) + "/*";
  }

  private static String getulevelPath(String day) throws ParseException {
    return "/tmp/20180501/*";
  }

  private static String getSql(String day) throws Exception {
    StringBuilder sql = new StringBuilder();

    BufferedReader reader = null;

    try {
      reader = new BufferedReader(
          new InputStreamReader(
              VideoBase.class.getClassLoader()
                  .getResourceAsStream("videobase.sql"),
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

    return sql.toString().replaceAll("_timestamp", String.valueOf(YYYYMMDD.parse(day).getTime()));
  }

  /**
   * 1.0, May 25, 2018
   * Main method
   */

  public static void main(String[] args) throws Exception {
    final String day = getToday(args);

    LOGGER.info("day: " + day);

    final String rawPath = getrawPath(day);

    LOGGER.info("rawPath: " + rawPath);

    final String ulevelPath = getulevelPath(day);

    final String kafkaServers = "first.kafka.dip.weibo.com:9092,second.kafka.dip.weibo.com:9092"
        + ",third.kafka.dip.weibo.com:9092,fourth.kafka.dip.weibo.com:9092"
        + ",fifth.kafka.dip.weibo.com:9092";

    final String kafkaTopic = "cdn_server";

    SparkConf conf = new SparkConf();

    JavaSparkContext context = new JavaSparkContext(conf);

    SparkSession session = SparkSession.builder().enableHiveSupport().getOrCreate();

    //创建raw视图
    String[] rawfieldNames = {"time_ex",
        "uid",
        "act",
        "vid",
        "uicode",
        "fid",
        "lfid",
        "luicode",
        "cardid",
        "lcardid",
        "featurecode",
        "from_",
        "wm",
        "old_wm",
        "ip",
        "logversion",
        "sysid",
        "ext",
        "valid_play_duration",
        "playduration",
        "oid"
    };

    List<StructField> fields = new ArrayList<>();

    for (String fieldName : rawfieldNames) {
      fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
    }

    StructType rawschema = DataTypes.createStructType(fields);

    JavaRDD<String> rawRdd = context.textFile(rawPath);

    JavaRDD<Row> rawrowRdd = rawRdd.map(line -> {
      String[] tempRow = line.split("`", -1);
      String[] row = new String[rawfieldNames.length];
      if (tempRow.length == 18) {
        System.arraycopy(tempRow, 0, row, 0, tempRow.length);

        Map<String,String> temp20 = new HashMap<>();
        for (String anaC : tempRow[17].split("\\|")) {
          String[] temps = anaC.split(":");
          if (Objects.equals(temps[0], "valid_play_duration")) {
            row[18] = temps[1];
          } else if (Objects.equals(temps[0], "playduration")) {
            row[19] = temps[1];
          } else if (Objects.equals(temps[0], "objectid") && temps.length == 3) {
            temp20.put("client",temps[1] + ":" + temps[2]);
          } else if (Objects.equals(temps[0], "objectid") && temps.length == 2) {
            temp20.put("server",temps[1]);
          }
        }
        row[20] = temp20.containsKey("client")
            ? temp20.get("client") : temp20.getOrDefault("server","");
      }

      return RowFactory.create((Object[]) row);
    }).filter(Objects::nonNull);

    Dataset<Row> rawDs = session.createDataFrame(rawrowRdd, rawschema);

    rawDs.createOrReplaceTempView("video_raw");

    //创建配置信息，用户等级视图
    String[] ulevelFilesNames = {"level", "uid"};

    List<StructField> ulevelfields = new ArrayList<>();

    for (String fieldName : ulevelFilesNames) {
      ulevelfields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
    }

    StructType ulevelSchema = DataTypes.createStructType(ulevelfields);

    JavaRDD<String> ulevelRdd = context.textFile(ulevelPath);

    JavaRDD<Row> ulevelrowRdd = ulevelRdd.map(line -> {

      String[] row = line.split("\t", -1);


      String[] values = new String[ulevelFilesNames.length];

      System.arraycopy(row, 0, values, 0, ulevelFilesNames.length);

      return RowFactory.create((Object[]) values);
    }).filter(Objects::nonNull);

    Dataset<Row> ulevelDs = session.createDataFrame(ulevelrowRdd, ulevelSchema);

    ulevelDs.createOrReplaceTempView("cfg_userLevel");

    Dataset<Row> resultDs = session.sql(getSql(day));

    resultDs.javaRDD().foreachPartition(iterator -> {
      Map<String, Object> config = new HashMap<>();

      config.put("bootstrap.servers", kafkaServers);
      config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

      Producer<String, String> producer = null;

      try {
        producer = new KafkaProducer<>(config);

        RateLimiter limiter = RateLimiter.create(1000000.0);

        while (iterator.hasNext()) {
          Row row = iterator.next();

          limiter.acquire(row.toString().getBytes().length);

          Map<String, Object> values = new HashMap<>();

          String[] rowFieldNames = row.schema().fieldNames();

          for (int index = 0; index < rowFieldNames.length; index++) {
            values.put(rowFieldNames[index], row.get(index));
          }

          try {
            producer.send(new ProducerRecord<>(kafkaTopic, GsonUtil.toJson(values)));
          } catch (Exception e) {
            LOGGER.debug("producer send record error: " + ExceptionUtils.getFullStackTrace(e));
          }
        }
      } catch (Exception e) {
        LOGGER.error("producer send error: " + ExceptionUtils.getFullStackTrace(e));
      } finally {
        if (producer != null) {
          producer.close();
        }
      }
    });

    context.stop();
  }

}
