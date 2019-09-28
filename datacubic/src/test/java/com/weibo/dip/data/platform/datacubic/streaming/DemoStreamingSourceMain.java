package com.weibo.dip.data.platform.datacubic.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yurun on 17/2/15.
 */
public class DemoStreamingSourceMain {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static String getLine(long now) {
        String timestamp = DATE_FORMAT.format(new Date(now));

        String ip = "58.214.240.114";

        String sessionid = "1486451934_2128611862";

        String action = "join_room";

        String uid = "2128611862";

        String clientfrom = "1070093010";

        String roomid = String.valueOf(now % 10);

        String containerid = "23091681aca7bc94c27c5d6dad6e13a151f212";

        String status = "1";

        String extend = "play_type=>live,online_num=>" + (now % 1000);

        return String.join("\t", timestamp, ip, sessionid, action, uid, clientfrom, roomid, containerid, status, extend);
    }

    public static void main(String[] args) throws Exception {
        Map<String, Object> config = new HashMap<>();

        config.put("bootstrap.servers", "d013004044.hadoop.dip.weibo.com:9092");

        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(config);

        String topic = "fulllink_result";

        long count = 0;

        while (true) {
            producer.send(new ProducerRecord<>(topic, getLine(System.currentTimeMillis())));

            Thread.sleep(1000);

            if (++count >= 100000) {
                break;
            }
        }

        producer.close();
    }

}
