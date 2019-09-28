package com.weibo.dip.data.platform.datacubic.videoupload;

import com.sina.dip.framework.util.IpUtil;
import com.sina.dip.iplibrary.IpToLocationService;
import com.sina.dip.iplibrary.Location;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.videoupload.util.DataProperties;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class VideoUploadStreamingV1 {

  private static final Logger LOGGER = LoggerFactory.getLogger(VideoUploadStreamingV1.class);

  private static final String ZKQUORUM = "zkstr";

  private static final String CONSUMERGROUP = "consumerGroup";

  private static final String RESULT_TOPIC = "kafka.topic.send";

  private static final String DURATION = "duration";

  private static final String RESOURCE_TOPICS = "resource_topics";

  private static final String PROCEDURCONFIG = "producerConfig";

  private static final IpToLocationService ipServer;

  private static SimpleDateFormat formatUtc = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  private static SimpleDateFormat target = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

  static {
    try {
      formatUtc.setTimeZone(TimeZone.getTimeZone("UTC"));
      ipServer = IpToLocationService.getInstance("/data0/dipplat/software/systemfile/iplibrary");
      //ipServer = IpToLocationService.getInstance("/Users/qianqian25/Documents/resource");
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static Map<String, Object> parserow(String row) throws Exception {
    Map<String, Object> line = GsonUtil.fromJson(row, GsonUtil.GsonType.OBJECT_MAP_TYPE);

    if (line.containsKey("trace.start") && line.get("trace.start") != null) {
      line.put("trace_start", formatUtc.format(target.parse((String) line.get("trace.start"))));
      line.put("timestamp", System.currentTimeMillis());
      line.put("business", "video-upload-v1");

      if (line.containsKey("info.uid")) {
        line.put("info.uid", String.valueOf(line.get("info.uid")));
      }

      if (line.containsKey("info.file_length")) {
        line.put("info.file_length", Float.valueOf(line.get("info.file_length").toString()));
      }

      if (line.containsKey("info.user_ip")) {
        String ip = (String) line.get("info.user_ip");

        if (!ip.isEmpty() && IpUtil.isIp(ip)) {
          Location location = ipServer.toLocation(ip);
          if (location != null) {
            line.put("info.country", location.getCountry());
            line.put("info.province", location.getProvince());
            line.put("info.city", location.getCity());
          } else {
            line.put("info.country", "");
            line.put("info.province", "");
            line.put("info.city", "");
          }
        } else {
          line.put("info.country", "");
          line.put("info.province", "");
          line.put("info.city", "");
        }
      }

      if (line.containsKey("info.response")) {
        Map<String, Object> response = (Map<String, Object>) line.get("info.response");
        if (response.containsKey("stored_id")) {
          response.put("stored_id", response.get("stored_id").toString());
        }
      }

      return line;

    } else {
      return null;
    }
  }

  /**
   * 1.0, May 9, 2018
   * Main method
   */

  public static void main(String[] args) throws Exception {
    Map<String, String> configMsg = DataProperties.loads();

    final String zkstr = configMsg.get(ZKQUORUM);

    final String consumerGroup = configMsg.get(CONSUMERGROUP);

    final String kafka_topic_send = configMsg.get(RESULT_TOPIC);

    final Long duration = Long.valueOf(configMsg.get(DURATION));

    final Map<String, Integer> resource_topics = GsonUtil.fromJson(configMsg.get(RESOURCE_TOPICS), GsonUtil.GsonType.INT_MAP_TYPE);

    final Map<String, Object> producerConfig = GsonUtil.fromJson(configMsg.get(PROCEDURCONFIG), GsonUtil.GsonType.OBJECT_MAP_TYPE);

    SparkConf sparkConf = new SparkConf();
    //SparkConf sparkConf = new SparkConf().setAppName("T2").setMaster("local[4]");

    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(duration));

    JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(ssc, zkstr, consumerGroup, resource_topics);

    JavaDStream<String> lines =
        messages.map((Function<Tuple2<String, String>, String>) Tuple2::_2);

    lines.foreachRDD((VoidFunction<JavaRDD<String>>) rdd ->
        rdd.foreachPartition((VoidFunction<Iterator<String>>) it -> {
          Producer<String, String> producer = new KafkaProducer<>(producerConfig);
          while (it.hasNext()) {
            String row = it.next();
            Map<String, Object> value;
            try {
              value = parserow(row);
            } catch (Exception e) {

              LOGGER.error("Parse Error count:" + " " + row + ExceptionUtils.getFullStackTrace(e));
              continue;
            }


            try {
              if (value != null && !value.isEmpty()) {
                producer.send(new ProducerRecord<>(kafka_topic_send, GsonUtil.toJson(value)));
                //System.out.println(String.valueOf(value));
              }
            } catch (KafkaException e) {
              LOGGER.error("kafka error: " + ExceptionUtils.getFullStackTrace(e));
              break;

            } catch (Exception e) {
              LOGGER.error("json to map error: " + value + ExceptionUtils.getFullStackTrace(e));
            }

          }
          producer.close();

        }));

    ssc.start();
    ssc.awaitTermination();
  }

}