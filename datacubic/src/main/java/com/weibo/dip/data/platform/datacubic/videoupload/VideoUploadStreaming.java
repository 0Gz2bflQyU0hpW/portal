package com.weibo.dip.data.platform.datacubic.videoupload;

import com.sina.dip.framework.util.IpUtil;
import com.sina.dip.iplibrary.IpToLocationService;
import com.sina.dip.iplibrary.Location;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.streaming.core.DipStreaming;
import com.weibo.dip.data.platform.datacubic.streaming.core.DipStreamingContext;
import com.weibo.dip.data.platform.datacubic.streaming.core.KafkaProducerProxy;
import com.weibo.dip.data.platform.datacubic.videoupload.util.DataProperties;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class VideoUploadStreaming {

    private static final Logger LOGGER = LoggerFactory.getLogger(VideoUploadStreaming.class);

    private static final String ZKQUORUM = "zkstr";

    private static final String CONSUMERGROUP = "consumerGroup";

    private static final String RESULT_TOPIC = "kafka.topic.send";

    private static final String DURATION = "duration";

    private static final String RESOURCE_TOPICS = "resource_topics";

    private static final String PROCEDURCONFIG = "producerConfig";

    private static class Parser implements VoidFunction<Iterator<String>> {
        private static final IpToLocationService ipServer;

        private SimpleDateFormat formatUtc = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        private SimpleDateFormat target = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

        private KafkaProducerProxy producer;

        private String kafka_topic_send;

        {
            formatUtc.setTimeZone(TimeZone.getTimeZone("UTC"));
        }

        static{
            try {

                ipServer = IpToLocationService.getInstance("/data0/dipplat/software/systemfile/iplibrary");
            } catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private Parser(KafkaProducerProxy producer, String kafka_topic_send) {
            this.producer = producer;
            this.kafka_topic_send = kafka_topic_send;
        }

        private Map<String, Object> parserow(String row) throws Exception {
            Map<String, Object> line = GsonUtil.fromJson(row, GsonUtil.GsonType.OBJECT_MAP_TYPE);

            if (line.containsKey("trace.start") && line.get("trace.start") != null) {
                line.put("trace_start", formatUtc.format(target.parse((String) line.get("trace.start"))));
                line.put("timestamp", System.currentTimeMillis());
                line.put("business", "video-upload-v1");

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

                line.put("uid",line.containsKey("uid") ? String.valueOf(line.get("uid"))
                    .split("\\.")[0] : null );

                return line;

            } else {
                return null;
            }
        }

        @Override
        public void call(Iterator<String> it) throws Exception {

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
                        producer.send(kafka_topic_send, GsonUtil.toJson(value));
                    }
                } catch (KafkaException e) {
                    LOGGER.error("kafka error: " + ExceptionUtils.getFullStackTrace(e));
                    break;

                } catch (Exception e) {
                    LOGGER.error("json to map error: " + value + ExceptionUtils.getFullStackTrace(e));
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> configMsg = DataProperties.loads();

        String zkstr = configMsg.get(ZKQUORUM);

        String consumerGroup = configMsg.get(CONSUMERGROUP);

        String kafka_topic_send = configMsg.get(RESULT_TOPIC);

        Long duration = Long.valueOf(configMsg.get(DURATION));

        Map<String, Integer> resource_topics = GsonUtil.fromJson(configMsg.get(RESOURCE_TOPICS), GsonUtil.GsonType.INT_MAP_TYPE);

        Map<String, Object> producerConfig = GsonUtil.fromJson(configMsg.get(PROCEDURCONFIG), GsonUtil.GsonType.OBJECT_MAP_TYPE);

        SparkConf sparkConf = new SparkConf();

        DipStreamingContext ssc = new DipStreamingContext(sparkConf, new Duration(duration));

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(ssc, zkstr, consumerGroup, resource_topics);

        JavaDStream<String> lines =
                messages.map((Function<Tuple2<String, String>, String>) Tuple2::_2);

        KafkaProducerProxy producer = new KafkaProducerProxy(sparkConf.get(DipStreaming.SPARK_APP_NAME), producerConfig);

        lines.foreachRDD((VoidFunction<JavaRDD<String>>) rdd -> rdd.foreachPartition(new Parser(producer, kafka_topic_send)));

        ssc.start();
        ssc.awaitTermination();
    }

}