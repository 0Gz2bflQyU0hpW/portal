package com.weibo.dip.data.platform.datacubic.fulllink;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.CharEncoding;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yurun on 17/5/10.
 */
public class SLAPusher {

    public static void main(String[] args) throws Exception {
        String kafkaZK = "d013004044.hadoop.dip.weibo.com:2181/kafka_intra";
        String kafkaConsumer = "fulllink_sla";
        String kafkaTopic = "dip-fulllink_result-common";

        String host = "10.39.40.39";
        int port = 2503;

        Properties properties = new Properties();

        properties.put("zookeeper.connect", kafkaZK);
        properties.put("group.id", kafkaConsumer);

        ConsumerConfig config = new ConsumerConfig(properties);

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<>();

        topicCountMap.put(kafkaTopic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(kafkaTopic);

        ConsumerIterator<byte[], byte[]> iterator = streams.get(0).iterator();

        //String pattern = "stats_byhost.fulllink.sla_test.byhost.10_13_4_44.%s.%s.%s.%s.%s.%s.%s.%s.%s %s %s\n";

        String pattern = "fulllink.sla_test.byhost.10_13_4_44.%s.%s.%s.%s.%s.%s %s %s\n";

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        while (iterator.hasNext()) {
            MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();

            String line = new String(messageAndMetadata.message());

            Map<String, Object> datas = GsonUtil.fromJson(line, GsonUtil.GsonType.OBJECT_MAP_TYPE);

            String subtype = (String) datas.get("subtype");
            String act = (String) datas.get("act");
            //String province = (String) datas.get("province");
            //String isp = (String) datas.get("isp");
            String app_version = ((String) datas.get("app_version")).replaceAll("\\.", "_");
            String system_version = ((String) datas.get("system_version")).replaceAll("\\.", "_");
            String network_type = (String) datas.get("network_type");
            //String request_url = ((String) datas.get("request_url")).replaceAll("\\.", "_");

            long during_time = ((Double) datas.get("during_time")).longValue();
            long net_time = ((Double) datas.get("net_time")).longValue();
            long parse_time = ((Double) datas.get("parse_time")).longValue();

            long timestamp = dateFormat.parse((String) datas.get("timestamp")).getTime() / 1000;

            String duringTimeLine = String.format(pattern, subtype, act, app_version, system_version, network_type, "during_time", String.valueOf(during_time), timestamp);
            String netTimeLine = String.format(pattern, subtype, act, app_version, system_version, network_type, "net_time", String.valueOf(net_time), timestamp);
            String parseTimeLine = String.format(pattern, subtype, act, app_version, system_version, network_type, "parse_time", String.valueOf(parse_time), timestamp);

            String data = duringTimeLine + netTimeLine + parseTimeLine;

            System.out.println(data);

            Socket socket = new Socket(host, port);

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), CharEncoding.UTF_8));

            writer.write(data);
            writer.flush();

            writer.close();

            socket.close();
        }

        consumer.shutdown();
    }

}
