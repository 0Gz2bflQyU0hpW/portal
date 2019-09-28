package com.weibo.dip.data.platform.datacubic.emailBusiness;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
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


public class VideoResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(VideoResource.class);

  private static final SimpleDateFormat YYYYMMDD = new SimpleDateFormat("yyyyMMdd");

  private static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");

  private static String getToday(String[] args) {
    return ArrayUtils.isNotEmpty(args) ? args[0] : YYYYMMDD.format(new Date());
  }

  private static String getrawPath(String day) throws ParseException {
    return "/user/hdfs/rawlog/www_weibovideowfpynt25az_videocommon/"
        + YYYY_MM_DD.format(YYYYMMDD.parse(day)) + "/*";
  }

  private static String getResourcePath(String day) throws ParseException {
    return "/user/hdfs/rawlog/www_weibovideowfpynt25az_systemvideologoyz/"
        + YYYY_MM_DD.format(YYYYMMDD.parse(day)) + "/*";
  }

  private static String getSql(String day) throws Exception {
    StringBuilder sql = new StringBuilder();

    BufferedReader reader = null;

    try {
      reader = new BufferedReader(
          new InputStreamReader(
              VideoResource.class.getClassLoader()
                  .getResourceAsStream("videoresource.sql"),
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

    final String resourcePath = getResourcePath(day);

    final String kafkaServers = "10.85.126.108:9092,10.85.126.109:9092,10.85.126.110:9092";

    final String kafkaTopic = "video_library";

    SparkConf conf = new SparkConf();

    JavaSparkContext context = new JavaSparkContext(conf);

    SparkSession session = SparkSession.builder().enableHiveSupport().getOrCreate();

    //视频推送给机器学习的视频信息
    String[] rawfieldNames = {"video_id"};

    List<StructField> fields = new ArrayList<>();

    for (String fieldName : rawfieldNames) {
      fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
    }

    StructType rawschema = DataTypes.createStructType(fields);

    JavaRDD<String> rawRdd = context.textFile(rawPath);

    JavaRDD<Row> rawrowRdd = rawRdd.map(line -> {
      String[] row = new String[rawfieldNames.length];

      Map<String, Object> lines = GsonUtil.fromJson(line, GsonUtil.GsonType.OBJECT_MAP_TYPE);

      Map<String, String> mata = GsonUtil.fromJson(GsonUtil.toJson(lines.get("meta")),
          GsonUtil.GsonType.STRING_MAP_TYPE);

      row[0] = mata.get("video_id");

      return RowFactory.create((Object[]) row);
    }).filter(Objects::nonNull);

    Dataset<Row> rawDs = session.createDataFrame(rawrowRdd, rawschema);


    rawDs.createOrReplaceTempView("video_raw");

    //处理水印识别结果数据
    String[] resourceFilesNames = {"logs"};

    List<StructField> resourcefields = new ArrayList<>();

    for (String fieldName : resourceFilesNames) {
      resourcefields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
    }

    StructType resourceSchema = DataTypes.createStructType(resourcefields);

    JavaRDD<String> resourceRdd = context.textFile(resourcePath);

    JavaRDD<Row> resourcerowRdd = resourceRdd.map(line -> {

      String[] values = {""};

      Map<String, Object> lines = GsonUtil.fromJson(line, GsonUtil.GsonType.OBJECT_MAP_TYPE);

      Map<String, Object>[] outputs = GsonUtil.fromJson(GsonUtil.toJson(lines.get("output")),
          GsonUtil.GsonType.OBJECT_LIST_TYPE);

      for (int i = 0; i < outputs.length; i++) {
        values[0] = values[0] + outputs[i].get("video_id") + ";";

        Map<String, String>[] logos = GsonUtil.fromJson(GsonUtil.toJson(outputs[i].get("logo")),
            GsonUtil.GsonType.OBJECT_LIST_TYPE);

        StringBuilder lg = new StringBuilder();

        for (Map<String, String> logo : logos) {
          if (!Objects.equals(logo.get("display_name"), "other")) {
            lg.append(logo.get("display_name")).append(",");
          }
        }
        if (!Objects.equals(lg.toString(), "")) {
          values[0] = values[0] + lg;
        } else {
          values[0] = values[0] + "other";
        }

        if (i < outputs.length - 1) {
          values[0] = (values[0] + "|").replace(",|", "|");
        }
      }


      return RowFactory.create((Object[]) values);
    }).filter(Objects::nonNull);

    Dataset<Row> ulevelDs = session.createDataFrame(resourcerowRdd, resourceSchema);

    ulevelDs.createOrReplaceTempView("cfg_resource");

    Dataset<Row> resultDs = session.sql(getSql(day));

    resultDs.javaRDD().repartition(1).saveAsTextFile("/tmp/videoresource");

    /**
     resultDs.javaRDD().foreachPartition(iterator -> {
     Map<String, Object> config = new HashMap<>();

     config.put("bootstrap.servers", kafkaServers);
     config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
     config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

     Producer<String, String> producer = null;

     try {
     producer = new KafkaProducer<>(config);

     RateLimiter limiter = RateLimiter.create(204800.0);

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
     LOGGER.error("producer send record error: " + ExceptionUtils.getFullStackTrace(e));
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
     */

    context.stop();
  }

}
