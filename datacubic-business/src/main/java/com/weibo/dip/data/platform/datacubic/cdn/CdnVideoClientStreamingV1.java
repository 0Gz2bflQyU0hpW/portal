package com.weibo.dip.data.platform.datacubic.cdn;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.cdn.util.DataProperties;
import com.weibo.dip.data.platform.datacubic.cdn.util.IpToLocation;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class CdnVideoClientStreamingV1 {
  private static final Logger LOGGER = LoggerFactory.getLogger(CdnVideoClientStreamingV1.class);

  private static final String ZKQUORUM = "zkstr";

  private static final String CONSUMERGROUP = "consumerGroup";

  private static final String RESULT_TOPIC = "kafka.topic.send";

  private static final String DURATION = "duration";

  private static final String RESOURCE_TOPICS = "resource_topics";

  private static final String PROCEDURCONFIG = "producerConfig";

  private static final String STREAMING_CONFIG = "cdnvideoclient.properties";

  private static final String RECEIVER_NUM = "receiverNum";

  private static String getSql() throws Exception {
    StringBuilder sql = new StringBuilder();

    BufferedReader reader = null;

    try {
      reader = new BufferedReader(new InputStreamReader(CdnVideoClientStreamingV1.class
          .getClassLoader().getResourceAsStream("cdn_client_video.sql"),
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

  private static final List<String> domainReg = Arrays.asList(
      "f.us.sinaimg.cn",
      "g.us.sinaimg.cn",
      "hd.us.sinaimg.cn",
      "s.us.sinaimg.cn",
      "v.us.sinaimg.cn",
      "wbvip.us.sinaimg.cn",
      "locallimit.us.sinaimg.cn",
      "s3.us.sinaimg.cn",
      "mp.us.sinaimg.cn",
      "public.us.sinaimg.cn",
      "us.sinaimg.cn"
  );

  private static Row parserow(String row) throws Exception {

    try {
      Map<String, Object> lrs = GsonUtil.fromJson(row.split("`")[0],
          GsonUtil.GsonType.OBJECT_MAP_TYPE);

      if (lrs.containsKey("video_cache_type") && Objects.equals(lrs.get("video_cache_type"), "1")
          && lrs.getOrDefault("video_url", null) != null) {
        final URL video_url = new URL((String) lrs.get("video_url"));
        String domain = video_url.getHost();
        if (domainReg.contains(domain)) {

          String cdn = lrs.containsKey("video_cdn") ? lrs.get("video_cdn").toString() : "";
          int indexstart = cdn.indexOf("f=");
          int indexend = cdn.indexOf(",");

          final Map<String, String> video_error_info =
              lrs.containsKey("video_error_info") ? GsonUtil.fromJson(GsonUtil
                      .toJson(lrs.get("video_error_info")),
                  GsonUtil.GsonType.STRING_MAP_TYPE) : null;

          return RowFactory.create(domain,
              video_url.getProtocol(),
              lrs.containsKey("ip") ? String.valueOf(lrs.get("ip")) : "",
              lrs.containsKey("video_firstframe_status") ? String.valueOf(
                  lrs.get("video_firstframe_status")) : "",
              lrs.containsKey("video_quit_status") ? String.valueOf(
                  lrs.get("video_quit_status")) : "",
              lrs.containsKey("video_firstframe_time") ? String.valueOf(
                  lrs.get("video_firstframe_time")) : "",
              lrs.containsKey("video_trace_dns_ip") ? String.valueOf(
                  lrs.get("video_trace_dns_ip")) : "",
              indexstart > -1 && indexend > -1 ? cdn.substring(indexstart + 2, indexend) : "",
              video_error_info != null ? String.valueOf(video_error_info.get("error_code")) : "");

        }
        return null;

      }
      return null;


    } catch (Exception e) {
      LOGGER.error("parse data error: {}", row);
      return null;
    }

  }

  /**1.0, May 9, 2018
   * Main method
   */

  public static void main(String[] args) throws Exception {
    final Map<String, String> configMsg = DataProperties.loads(STREAMING_CONFIG);

    final String zkstr = configMsg.get(ZKQUORUM);

    final String consumerGroup = configMsg.get(CONSUMERGROUP);

    final String kafka_topic_send = configMsg.get(RESULT_TOPIC);

    final Long duration = Long.valueOf(configMsg.get(DURATION));

    final int receiver_num = Integer.valueOf(configMsg.get(RECEIVER_NUM));

    final Map<String, Integer> resource_topics = GsonUtil.fromJson(configMsg.get(RESOURCE_TOPICS),
        GsonUtil.GsonType.INT_MAP_TYPE);

    final Map<String, Object> producerConfig = GsonUtil.fromJson(configMsg.get(PROCEDURCONFIG),
        GsonUtil.GsonType.OBJECT_MAP_TYPE);

    SparkConf sparkConf = new SparkConf();

    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, new Duration(duration));

    List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(2);

    for (int i = 0; i < receiver_num; i++) {
      kafkaStreams.add(KafkaUtils.createStream(ssc, zkstr, consumerGroup, resource_topics, StorageLevel.MEMORY_AND_DISK_SER()));
    }

    JavaDStream<String> lines = ssc.union(kafkaStreams.get(0),
        kafkaStreams.subList(1, kafkaStreams.size())).map(Tuple2::_2);


    SparkSession session = SparkSession.builder().getOrCreate();
    session.udf().register("ipToLocation", new IpToLocation(), DataTypes.createMapType(DataTypes
        .StringType, DataTypes.StringType));

    String[] fieldNames = {"domain", "protocal", "ip", "video_firstframe_status",
        "video_quit_status", "video_firstframe_time", "video_trace_dns_ip", "cdn", "error_code"};
    List<StructField> fields = new ArrayList<>();
    for (String fieldName : fieldNames) {
      fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
    }
    StructType schema = DataTypes.createStructType(fields);

    JavaDStream<Row> rowDstream = lines.map((Function<String, Row>) CdnVideoClientStreamingV1
        ::parserow).filter(Objects::nonNull);

    rowDstream.foreachRDD((VoidFunction<JavaRDD<Row>>) (JavaRDD<Row> rowRdd) -> {

      Dataset<Row> sourceDs = session.createDataFrame(rowRdd, schema);
      sourceDs.createOrReplaceTempView("cdn_client_video");

      Dataset<Row> resultDs = session.sql(getSql());

      LongAccumulator outputCount = sparkContext.sc().longAccumulator("outputCount");

      resultDs.javaRDD().foreachPartition((Iterator<Row> iterator) -> {
        Producer<String, String> producer = null;
        try {
          producer = new KafkaProducer<>(producerConfig);
          while (iterator.hasNext()) {
            Row row = iterator.next();

            Map<String, Object> values = new HashMap<>();

            values.put("timestamp", System.currentTimeMillis());

            String[] rowFieldNames = row.schema().fieldNames();

            for (int index = 0; index < rowFieldNames.length; index++) {
              values.put(rowFieldNames[index], row.get(index));
            }

            try {
              producer.send(new ProducerRecord<>(kafka_topic_send, GsonUtil.toJson(values)));
              outputCount.add(1L);
            } catch (Exception e) {
              LOGGER.error("producer send record error: " + values + ExceptionUtils
                  .getFullStackTrace(e));
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

      LOGGER.info("output count: " + outputCount.value());


    });

    ssc.start();
    ssc.awaitTermination();
  }
}