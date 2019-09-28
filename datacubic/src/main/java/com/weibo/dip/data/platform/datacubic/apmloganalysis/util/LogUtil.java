package com.weibo.dip.data.platform.datacubic.apmloganalysis.util;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class LogUtil extends TimerTask{

    public static Queue<String> queue = new LinkedList<>();

    public void addLog(String elem){
        queue.add(elem);
    }

    @Override
    public void run() {
        Producer<String, String> producer = KafkaProducerUtil.getInstance("10.13.4.44:9092");

        for(int index=0; index<5; index++){

            try{
                String log = queue.remove();
                Map<String, String> map = GsonUtil.fromJson(log, GsonUtil.GsonType.STRING_MAP_TYPE);
                producer.send(new ProducerRecord<>("dip-kafka2es-common", GsonUtil.toJson(map)));
            } catch (NoSuchElementException e) {
                break;
            }
        }
    }

}
