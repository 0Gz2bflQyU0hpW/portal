package com.weibo.dip.cdn;

import com.sina.dip.framework.util.IpUtil;
import com.sina.dip.iplibrary.IpToLocationService;
import com.sina.dip.iplibrary.Location;
import com.weibo.dip.cdn.util.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.net.URL;
import java.util.*;
import java.util.regex.Pattern;

public class CDNPicClientStreaming {

    private static final Logger LOGGER = LoggerFactory.getLogger(CDNPicClientStreaming.class);

    private static final String ZKQUORUM = "zkstr";

    private static final String CONSUMERGROUP = "consumerGroup";

    private static final String RESULT_TOPIC = "kafka.topic.send";

    private static final String DURATION = "duration";

    private static final String RESOURCE_TOPICS = "resource_topics";

    private static final String PROCEDURCONFIG = "producerConfig";

    private static final String STREAMING_CONFIG = "picclient.properties";

    private static class Parser implements VoidFunction<Iterator<String>> {
        private static final IpToLocationService ipServer;

        private static final String DomainReg = "wx[1,4]|ww[1,4]|tva[1,4]|tvax[1,4]";

        private KafkaProducerProxy producer;

        private String kafka_topic_send;

        static {
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

            Map<String, Object> lrs = GsonUtil.fromJson(row.substring(row.indexOf(" ") + 1, row.length()), GsonUtil.GsonType.OBJECT_MAP_TYPE);

            Map<String, Object> line = new HashMap<>();

            if (lrs.getOrDefault("pic_url", null) == null || !lrs.getOrDefault("pic_url", null).toString().startsWith("h")) {
                return null;
            } else {
                URL pic_url = new URL((String) lrs.get("pic_url"));

                if (Pattern.compile(DomainReg).matcher(pic_url.getHost().split("\\.")[0]).find()) {
                    line.put("timestamp", System.currentTimeMillis());

                    line.put("domain", pic_url.getHost());

                    line.put("protocal", pic_url.getProtocol());

                    Map<String, String> response_header = lrs.containsKey("response_header") ?
                            GsonUtil.fromJson(GsonUtil.toJson(lrs.get("response_header")), GsonUtil.GsonType.STRING_MAP_TYPE) : null;
                    String cdn = response_header != null ? response_header.getOrDefault("X-Via-CDN", "") : "";
                    line.put("cdn", cdn.contains("f=") && cdn.contains(",") ?
                            cdn.substring(cdn.indexOf("f=") + 2, cdn.indexOf(",")) : "");

                    String ip = (String) lrs.getOrDefault("ip", "");
                    if (!ip.isEmpty() && IpUtil.isIp(ip)) {
                        Location location = ipServer.toLocation(ip);
                        line.put("province", location.getProvince());
                        line.put("isp", location.getIsp());
                    } else {
                        line.put("province", "");
                        line.put("isp", "");
                    }

                    String error_code = lrs.containsKey("error_code") ? lrs.get("error_code").toString() : "";
                    if (error_code.startsWith("2") || error_code.startsWith("3")) {
                        line.put("business", "dip-cdn-client-picture-detail-normal");
                    } else {
                        line.put("business", "dip-cdn-client-picture-detail-abnormal");
                        line.put("error_code", error_code);
                    }

                    line.put("dst_ip", lrs.getOrDefault("dst_ip", ""));

                    line.put("download_time", lrs.getOrDefault("download_time", ""));

                    return line;
                } else {
                    return null;
                }

            }

        }

        @Override
        public void call(Iterator<String> it) throws Exception {

            while (it.hasNext()) {

                String row = it.next();
                Map<String, Object> value;
                try {
                    value = parserow(row);
                    System.out.println(value);
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
        Map<String, String> configMsg = DataProperties.loads(STREAMING_CONFIG);

        String zkstr = configMsg.get(ZKQUORUM);

        String consumerGroup = configMsg.get(CONSUMERGROUP);

        String kafka_topic_send = configMsg.get(RESULT_TOPIC);

        Long duration = Long.valueOf(configMsg.get(DURATION));

        Map<String, Integer> resource_topics = GsonUtil.fromJson(configMsg.get(RESOURCE_TOPICS),
                GsonUtil.GsonType.INT_MAP_TYPE);

        Map<String, Object> producerConfig = GsonUtil.fromJson(configMsg.get(PROCEDURCONFIG),
                GsonUtil.GsonType.OBJECT_MAP_TYPE);

        SparkConf sparkConf = new SparkConf();

        DipStreamingContext ssc = new DipStreamingContext(sparkConf, new Duration(duration));

        List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<JavaPairDStream<String, String>>(2);

        for (int i = 0; i < 2; i++)
            kafkaStreams.add(KafkaUtils.createStream(ssc, zkstr, consumerGroup, resource_topics));

        JavaDStream<String> lines = ssc.union(kafkaStreams.get(0),
                kafkaStreams.subList(1, kafkaStreams.size())).map(Tuple2::_2);

        KafkaProducerProxy producer = new KafkaProducerProxy(sparkConf.get(DipStreaming.SPARK_APP_NAME), producerConfig);

        lines.foreachRDD((VoidFunction<JavaRDD<String>>) (JavaRDD<String> rdd) -> {
            rdd.foreachPartition(new Parser(producer, kafka_topic_send));
        });

        ssc.start();
    }
}
