package com.weibo.dip.data.platform.datacubic.Kafka;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.commons.lang.CharEncoding;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by yurun on 17/2/20.
 */
public class IosCrashSource2 {

    public static String getLine() throws IOException {
        StringBuilder buffer = new StringBuilder();

        BufferedReader reader = new BufferedReader(new InputStreamReader(IosCrashSource2.class
            .getClassLoader()
            .getResourceAsStream("ios_crash2.log"), CharEncoding.UTF_8));

        String line = null;

        while ((line = reader.readLine()) != null) {
            buffer.append(line);
        }

        reader.close();

        return GsonUtil.toJson(GsonUtil.fromJson(buffer.toString(), GsonUtil.GsonType.OBJECT_MAP_TYPE));
    }

    public static void main(String[] args) throws Exception {
        String line = getLine();

        System.out.println(line);

        //        KafkaProducerProxy producer = new KafkaProducerProxy(
        //            new String[]{"d013004044.hadoop.dip.weibo.com:9092"});
        //
        //        String topic = "demo_streaming_source";
        //
        //        long count = 0;
        //
        //        while (true) {
        //            producer.send(topic, line);
        //
        //            if (++count >= 10000) {
        //                break;
        //            }
        //
        //            Thread.sleep(1000);
        //        }
    }

}
