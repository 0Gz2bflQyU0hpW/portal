package com.weibo.dip.data.platform.datacubic.apmloganalysis;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.apmloganalysis.util.KafkaProducerUtil;
import com.weibo.dip.data.platform.datacubic.streaming.udf.fulllink.ParseUAInfo;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class ApmPropertyLog implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApmPropertyLog.class);

  //读日志
  private static String getZookeeperServers() {
    return "10.41.15.21:2181,10.41.15.22:2181,10.41.15.23:2181";
  }

  private static String getConsumerGroup() {
    return "apm_property";
  }

  private static String getInPutTopic() {
    return "mweibo_client_weibo_apm_log";
  }

  //性能日志
  private static String getProcessedBrokers() {
    return "first.kafka.dip.weibo.com:9092,second.kafka.dip.weibo.com:9092,third.kafka.dip.weibo.com:9092,fourth.kafka.dip.weibo.com:9092,fifth.kafka.dip.weibo.com:9092";
  }

  private static String getProcessedTopic() {
    return "dip-kafka2es-common";
  }

  //写原始日志
  private static String getOriginBrokers() {
    return "10.41.15.21:9092,10.41.15.22:9092,10.41.15.23:9092,10.41.15.24:9092,10.41.15.25:9092,10.41.15.26:9092,10.41.15.27:9092,10.41.15.28:9092,10.41.15.29:9092,10.41.15.30:9092";
  }

  private static String getOriginTopic() {
    return "mweibo_client_weibo_apm_log_toes";
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Integer> getSourceTopicMap() {
    Map<String, Integer> map = new HashMap();
    map.put(getInPutTopic(), 4);
    return map;
  }

  public static void main(String[] args) throws Exception {

    ApmPropertyLog apm = new ApmPropertyLog();

    SparkConf conf = new SparkConf();

    JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(10000));

    List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(2);
    for (int i = 0; i < 2; i++) {
      kafkaStreams
          .add(KafkaUtils.createStream(streamingContext, getZookeeperServers(), getConsumerGroup(),
              getSourceTopicMap(), StorageLevel.MEMORY_AND_DISK_SER()));
    }
    JavaPairDStream<String, String> messages = streamingContext
        .union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));

    JavaDStream<String> msgDStream = messages.map((Function<Tuple2<String, String>, String>) Tuple2::_2);

    JavaDStream<Map<String, Object>> mapDStream = apm.parseRow(msgDStream);

    apm.send2Kafka(mapDStream);

    streamingContext.start();
    streamingContext.awaitTermination();
  }

  private void send2Kafka(JavaDStream<Map<String, Object>> mapDStream) {

    mapDStream.foreachRDD(rdd -> rdd.foreachPartition(iterator -> {

      @SuppressWarnings("unchecked")
      Producer<String, String> producer_origin = KafkaProducerUtil.getInstance(getOriginBrokers());
      @SuppressWarnings("unchecked")
      Producer<String, String> producer = KafkaProducerUtil.getInstance(getProcessedBrokers());

      while (iterator.hasNext()) {
        Map<String, Object> map = iterator.next();

        map.put("business", "fulllink-apm-summary");
        map.put("timestamp", System.currentTimeMillis());

        producer_origin.send(new ProducerRecord<>(getOriginTopic(), GsonUtil.toJson(map)));
        producer.send(new ProducerRecord<>(getProcessedTopic(), GsonUtil.toJson(map)));
      }
    }));
  }

  //所有空记录在这里过滤掉
  private JavaDStream<Map<String, Object>> parseRow(JavaDStream<String> msgDStream) {

    JavaDStream<Map<String, Object>> obj = msgDStream.map(row -> {

      Map<String, Object> rowmap;
      try {
        rowmap = GsonUtil.fromJson(row, GsonUtil.GsonType.OBJECT_MAP_TYPE);
      } catch (Exception e) {
        LOGGER.error("log parse error : ", row);
        LOGGER.error(ExceptionUtils.getFullStackTrace(e));
        return null;
      }

      Map<String, String> ua;
      ParseUAInfo parseUAInfo = new ParseUAInfo();
      try {
        ua = parseUAInfo.call((String) rowmap.get("ua"));
      } catch (Exception e) {
        LOGGER.error("line " + row);
        LOGGER.error(ExceptionUtils.getFullStackTrace(e));
        return null;
      }

      if (ua == null) {
        return null;
      }

      String uid = String.valueOf(rowmap.get("uid"));
      if (uid.length() < 9) {
        LOGGER.error("uid length error : ", row);
        return null;
      }

      String type = String.valueOf(rowmap.get("type")).toLowerCase();
      if ("flick".equals(type)) {
        return null;
      }

      String from_ = (String) rowmap.get("from");
      String system = from_.substring(from_.length() - 5, from_.length());

      Map<String, Object> line = new HashMap<>();

      line.put("uid", uid);
      line.put("partial_uid", uid.substring(uid.length() - 9, uid.length() - 7));
      line.put("type_", type);

      if ("android".equals(ua.get("system"))) {
        line.put("model", ua.get("mobile_model"));
      } else if (!"NULL".equals(ua.get("mobile_model"))) {
        line.put("model", ua.get("mobile_manufacturer") + "," + ua.get("mobile_model"));
      } else {
        line.put("model", ua.get("mobile_manufacturer"));
      }
      line.put("system_version", ua.get("system_version"));
      line.put("from_", from_);
      line.put("net_type", null != rowmap.get("networktype") ? rowmap.get("networktype") : "NULL");
      line.put("uicode", null != rowmap.get("uicode") ? rowmap.get("uicode") : "NULL");

      if ("95010".equals(system)) {
        line.put("system", "android");
      } else if ("93010".equals(system)) {
        line.put("system", "iphone");
      } else {
        line.put("system", "NULL");
      }

      line.put("client_log_timestamp", rowmap.get("client_log_timestamp"));
      line.put("total_num", 1);

      Object tsServerObj = rowmap.get("@timestamp");
      if (tsServerObj != null) {
        line.put("@timestamp", String.valueOf(tsServerObj));
      }

      line.put("max", rowmap.get("max"));
      line.put("min", rowmap.get("min"));
      line.put("median", rowmap.get("median"));
      line.put("per5", rowmap.get("fp"));
      line.put("per95", null != rowmap.get("nfp") ? rowmap.get("nfp") : rowmap.get("np"));
      if (null != rowmap.get("ave")) {
        line.put("avg", rowmap.get("ave"));
      } else {
        line.put("avg", rowmap.get("avg"));
      }

      return line;
    });

    return obj.filter(Objects::nonNull);
  }

}
