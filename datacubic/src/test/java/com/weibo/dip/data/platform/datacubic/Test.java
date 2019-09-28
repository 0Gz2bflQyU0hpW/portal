package com.weibo.dip.data.platform.datacubic;

import com.weibo.dip.data.platform.datacubic.streaming.util.KafkaProducerFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by xiaoyu on 2017/5/4.
 */
public class Test {

    public static void main(String[] args) throws InterruptedException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(0);
        System.out.println(calendar.getTime());

    }
}
