package com.weibo.dip.data.platform.falcon.transport;

import com.weibo.dip.data.platform.falcon.transport.task.AdditionProducer;
import com.weibo.dip.data.platform.falcon.transport.task.Consumer;
import com.weibo.dip.data.platform.falcon.transport.task.ProduceProducer;
import com.weibo.dip.data.platform.falcon.transport.task.Producer;
import com.weibo.dip.data.platform.falcon.transport.utils.ConfigUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by Wen on 2017/2/20.\
 */
public class HDFSToKafkaMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSToKafkaMain.class);

    private static final String PRODUCE = "produce";
    private static final String ADDITION = "addition";

    private static class CloseThread implements Runnable {
        private Producer producer;
        private Consumer consumer;

        private CloseThread(Producer producer, Consumer consumer) {
            this.producer = producer;
            this.consumer = consumer;
        }

        @Override
        public void run() {
//            String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            File file = new File(ConfigUtils.STOP_FILE_PATH);

            while (true) {
                if (!file.exists()) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        continue;
                    }
                    continue;
                }

                try {
                    producer.stop();

                    LOGGER.info("Producer has been stopped");
                } catch (Exception e) {
                    LOGGER.error("Producer stop error: " + ExceptionUtils.getFullStackTrace(e));
                }

                try {
                    consumer.stop();

                    LOGGER.info("Consumer has been stopped");

                } catch (Exception e) {
                    LOGGER.error("Consumer stop error: " + ExceptionUtils.getFullStackTrace(e));
                }
                return;
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            LOGGER.error("HDFSToKafkaMain needs an argument...(produce or addition)");
            return;
        }

        Producer producer;
        Consumer consumer = new Consumer();

        if (PRODUCE.equals(args[0])) {
            producer = new ProduceProducer();
        } else if (ADDITION.equals(args[0])) {
            producer = new AdditionProducer(consumer);
        } else {
            LOGGER.error("HDFSToKafkaMain needs an argument...(produce or addition)");
            return;
        }

        Thread closeThread = new Thread(new CloseThread(producer, consumer));
        closeThread.setDaemon(true);
        closeThread.start();

        try {
            producer.start();
            consumer.start();
        } catch (Exception e) {
            LOGGER.error("HDFS to Kafka start error: " + ExceptionUtils.getFullStackTrace(e));
        }
    }

}

