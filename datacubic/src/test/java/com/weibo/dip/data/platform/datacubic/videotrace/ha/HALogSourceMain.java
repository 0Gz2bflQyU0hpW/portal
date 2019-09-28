package com.weibo.dip.data.platform.datacubic.videotrace.ha;

import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yurun on 17/1/19.
 */
public class HALogSourceMain {

    private static final FileSystem FILE_SYSTEM;

    static {
        try {
            FILE_SYSTEM = FileSystem.get(new Configuration());
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static void writeToKafka(Path path) throws Exception {
        Map<String, Object> config = new HashMap<>();

        config.put("bootstrap.servers", "d013004044.hadoop.dip.weibo.com:9092");

        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(config);

        String topic = "videotrace_ha";

        BufferedReader reader = new BufferedReader(new InputStreamReader(FILE_SYSTEM.open(path), CharEncoding.UTF_8));

        String line;

        while ((line = reader.readLine()) != null) {
            line = line.trim();

            if (StringUtils.isEmpty(line)) {
                continue;
            }

            producer.send(new ProducerRecord<>(topic, line));
        }

        reader.close();

        producer.close();
    }

    public static void main(String[] args) throws Exception {
        Path input = new Path("/user/hdfs/rawlog/app_picserversweibof6vwt_cachel2ha/2017_02_07/00");

        Path[] paths = FileUtil.stat2Paths(FILE_SYSTEM.listStatus(input));

        for (int index = 0; index <= paths.length; index++) {
            writeToKafka(paths[index]);

            System.out.println("write " + (index + 1) + "[" + paths.length + "] finished");
        }
    }

}
