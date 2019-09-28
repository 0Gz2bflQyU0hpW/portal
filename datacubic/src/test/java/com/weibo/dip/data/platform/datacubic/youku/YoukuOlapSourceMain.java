package com.weibo.dip.data.platform.datacubic.youku;

import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yurun on 16/12/29.
 */
public class YoukuOlapSourceMain {

    public static void main(String[] args) throws Exception {
        Map<String, Object> config = new HashMap<>();

        config.put("bootstrap.servers", "d013004044.hadoop.dip.weibo.com:9092");

        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(config);

        String topic = "druid_example_topic";

        List<String> lines = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/yurun/Downloads/app_weibomobilekafka1234_weibomobileaction799-172.16.140.83-29549-2016_12_28_09-2016122809117_1747926"), CharEncoding.UTF_8));

        String line;

        while ((line = reader.readLine()) != null) {
            line = line.trim();

            if (StringUtils.isEmpty(line)) {
                continue;
            }

            lines.add(line);
        }

        reader.close();

        while (true) {
            for (String data : lines) {
                producer.send(new ProducerRecord<>(topic, data));
            }
        }
    }

}
