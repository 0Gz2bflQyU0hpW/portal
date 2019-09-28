package com.weibo.dip.data.platform.datacubic.streaming.demo;

import com.weibo.dip.data.platform.datacubic.streaming.util.KafkaProducerProxy;

/**
 * Created by yurun on 17/2/20.
 */
public class DemoStreamingSource {

    public static void main(String[] args) throws Exception {
        KafkaProducerProxy producer = new KafkaProducerProxy(
            new String[]{"d013004044.hadoop.dip.weibo.com:9092"});

        String topic = "demo_streaming_source";

        long count = 0;

        while (true) {
            producer.send(topic, String.valueOf(System.currentTimeMillis()));

            if (++count >= 10000) {
                break;
            }

            Thread.sleep(1000);
        }
    }

}
