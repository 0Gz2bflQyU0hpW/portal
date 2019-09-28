package com.weibo.dip.data.platform.datacubic.videotrace.ha;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by yurun on 16/12/29.
 */
public class YoukuOlapSourceMain2 {

    public static void main(String[] args) throws Exception {
        Map<String, Object> config = new HashMap<>();

        config.put("bootstrap.servers", "d013004044.hadoop.dip.weibo.com:9092");

        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(config);

        String topic = "videotrace_ha_result";

        while (true) {
            Map<String, Object> datas = new HashMap<>();

            datas.put("size", 1234);
            datas.put("domain", "ww3.sinaimg.cn");
            datas.put("isp", "isp");
            datas.put("idc", "idc");
            datas.put("cdn", "cdn");

            SimpleDateFormat target = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

            target.setTimeZone(TimeZone.getTimeZone("UTC"));

            datas.put("timestamp", target.format(new Date()));

            producer.send(new ProducerRecord<>(topic, GsonUtil.toJson(datas)));
        }
    }

}
