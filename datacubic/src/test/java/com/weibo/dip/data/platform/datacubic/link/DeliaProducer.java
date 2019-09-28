package com.weibo.dip.data.platform.datacubic.link;

import com.weibo.dip.data.platform.datacubic.demo.FileUtil;
import com.weibo.dip.data.platform.datacubic.streaming.util.KafkaProducerFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;

/**
 * Created by delia on 2017/2/23.
 */public class DeliaProducer {
     private static String TOPIC = "link";
    public static void main(String[] args) throws Exception {
        Producer<String, String> producer = KafkaProducerFactory.getProducer("10.210.136.34:9092,10.210.136.80:9092,10.210.77.15:9092");
        for(;;) {
            BufferedReader reader = FileUtil.getFileReader("/Users/delia/Desktop/filterlog");
            String str = null;
            while ((str = reader.readLine()) != null) {
                System.out.println(str);
                producer.send(new ProducerRecord<String, String>(TOPIC,str));
                Thread.sleep(1000);
            }
            reader.close();
        }
    }
}

