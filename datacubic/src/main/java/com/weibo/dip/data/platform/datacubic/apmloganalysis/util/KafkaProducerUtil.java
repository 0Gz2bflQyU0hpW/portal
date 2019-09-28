package com.weibo.dip.data.platform.datacubic.apmloganalysis.util;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaProducerUtil implements Serializable {

    private static Map<String, Producer> map = new HashMap<String, Producer>();

    /**
    private static BlockingQueue<Map<String, Object>> queue = new LinkedBlockingQueue<>(2000000);

    static {
        Timer timer = new Timer("func-cost-timer");
        timer.schedule(new LogTimerTask(), 10000l, 10000l);
    }*/


    // TODO 创建map<topic, producer>
    public static Producer getInstance(String brokerList) {

        if (map.get(brokerList) == null) {
            synchronized (Producer.class){
                if (map.get(brokerList) == null) {

                    Map<String, Object> config = new HashMap<>();

                    config.put("bootstrap.servers", brokerList);
                    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                    Producer producer = new KafkaProducer<>(config);

                    map.put(brokerList, producer);
                }
            }
        }

        return map.get(brokerList);
    }


    /**
    public static void addMessage(Map<String, Object> map) throws Exception {
        queue.put(map);
    }

    private static class LogTimerTask extends TimerTask {

        @Override
        public void run() {

            Producer<String, String> producer = KafkaProducerUtil.getInstance("10.13.4.44:9092");

            while (queue.peek() != null){
                try{
                    Map<String, Object> log = queue.take();
                    producer.send(new ProducerRecord<>("test", GsonUtil.toJson(log)));
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

    }*/



}
