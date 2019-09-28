package com.weibo.dip.data.platform.datacubic.Kafka;

import com.weibo.dip.data.platform.datacubic.streaming.util.KafkaProducerProxy;
import org.apache.commons.lang.CharEncoding;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by yurun on 17/2/20.
 */
public class IosCrashSource {

    public static String getLine() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(IosCrashSource.class.getClassLoader()
            .getResourceAsStream("ios_crash.log"), CharEncoding.UTF_8));

        String line = reader.readLine();

        reader.close();

        return line;
    }

    public static void main(String[] args) throws Exception {
        String line = getLine();

        KafkaProducerProxy producer = new KafkaProducerProxy(
            new String[]{"d013004044.hadoop.dip.weibo.com:9092"});

        String topic = "demo_streaming_source";

        long count = 0;

        while (true) {
            producer.send(topic, line);

            if (++count >= 10000) {
                break;
            }

            Thread.sleep(1000);
        }
    }

}
