package com.weibo.dip.data.platform.falcon.kafka.Service.test;

import com.weibo.dip.data.platform.falcon.kafka.Service.KafkaService;


/**
 * Created by Wen on 2016/12/23.
 *
 */
public class TestKafka {
    public static void main(String[] args) throws Exception {
        new KafkaService().getRecentNumMessage("10.210.136.61:2181,10.210.136.62:2181,10.210.136.64:2181/kafka/test01","wen","aaa",10);

    }
}
