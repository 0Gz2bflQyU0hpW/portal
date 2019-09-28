package com.weibo.dip.databus.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jianhong1 on 2018/2/5.
 */
public class ProduceTestMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProduceTestMain.class);

    private static final String BORKERS = "d056077.eos.dip.sina.com.cn:9090,d056083.eos.dip.sina.com.cn:9090,d077114141.dip.weibo.com:9090";
    private static final String TOPIC = "test1";
    private static int THREADS = 2;

    public static class ProducerClient implements Runnable {
        private String brokers;
        private String topic;

        public ProducerClient(String brokers, String topic) {
            this.brokers = brokers;
            this.topic = topic;
        }

        public void run() {
            LOGGER.info("Current Thread Name: " + Thread.currentThread().getName());

            Map<String, Object> config = new HashMap<>();
            config.put("bootstrap.servers", brokers);
            config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<>(config);
            while (true){
                StringBuilder sb = new StringBuilder();
                String date = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss SSS").format(new Date());
                String str = String.format("%s_%s_%s", Thread.currentThread().getName(), this.getClass().getCanonicalName(), date);
                sb.append(str);

                long startTime = System.currentTimeMillis();
                try {
                    producer.send(new ProducerRecord<>(topic, sb.toString()), new DemoCallBack(startTime));
                }catch (Exception e){
                    LOGGER.error("producer client exception: " + e.toString());
                }
            }
        }
    }

    public static class DemoCallBack implements Callback{
        private final long startTime;

        public DemoCallBack(long startTime){
            this.startTime = startTime;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if(e != null){
                LOGGER.error("write kafka error, partition:{}, offset:{}, elapsedTime:{}ms\n{}",
                    metadata.partition(), metadata.offset(), elapsedTime, ExceptionUtils.getFullStackTrace(e));
            }else{
//                LOGGER.info("partition:{}, offset:{}, elapsedTime:{}ms", metadata.partition(), metadata.offset(), elapsedTime);
            }
        }
    }

    public static void main(String[] args) {
        if(args.length > 0){
            THREADS = Integer.valueOf(args[0]);
        }
        LOGGER.info("thread number: {}, topic: {}", THREADS, TOPIC);

        Runnable myRunnable = new ProducerClient(BORKERS, TOPIC);
        for (int i = 0; i < THREADS; i++) {
            new Thread(myRunnable).start();
        }
    }
}


