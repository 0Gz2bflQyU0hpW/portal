package com.weibo.dip.databus.kafka;

import com.weibo.dip.databus.kafka.utils.DateFormat;
import com.weibo.dip.databus.kafka.utils.HDFSUtils;
import com.weibo.dip.databus.kafka.utils.KafkaProperties;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class KafkaToHDFS {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToHDFS.class);
    private static final String DATASET;

    private static Properties properties;
    private static String hostname = "localhost";

    static{
        properties = KafkaProperties.getInstance();
        DATASET = properties.getProperty("dataset");
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOGGER.error("can't get hostname \n{}", ExceptionUtils.getFullStackTrace(e));
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * 拼接filepath(hdfs)
     * @return
     */
    public static String getFilePath(Date date){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("/user/hdfs/rawlog/").append(DATASET).append("/").append(DateFormat.getCurrentDate(date)).append("/").append(DateFormat.getCurrentHour(date)).append("/")
                .append(DATASET).append("-").append(hostname).append("-").append(DateFormat.getCurrentDate(date)).append("_").append(DateFormat.getCurrentTime(date))
                .append("-").append(Thread.currentThread().getName());

        return stringBuilder.toString();
    }


    public static class KafkaConsumerTask implements Runnable{
        private final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        private static String checkFilePath;

        public KafkaConsumerTask(String checkFilePath){
            this.checkFilePath = checkFilePath;
        }

        private File getCheckFile() {
            File checkFile;
            if(checkFilePath == null) {
                // 若指定checkFilePath不存在，则加载默认checkFile
                checkFile= new File(System.getProperty("user.dir") + "/databus-kafka/stopped-task/modifyme");
            }else{
                checkFile = new File(checkFilePath);
            }

            return checkFile;
        }

        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            LOGGER.info("{} has started", threadName);

            consumer.subscribe(Arrays.asList(properties.getProperty("topic")));

            String dateHour = null;
            BufferedWriter bufferedWriter = null;

            File checkFile = getCheckFile();
            long modifiedTime = checkFile.lastModified();
            try {
                // check file whether modified，if modified then stop KafkaConsumerTask
                while(modifiedTime == checkFile.lastModified()) {
                    //若是下一个小时则在hdfs新建文件
                    Date date = new Date();
                    if (!DateFormat.getCurrentDateHour(date).equals(dateHour)) {
                        if(bufferedWriter != null){
                            bufferedWriter.close();
                        }

                        dateHour = DateFormat.getCurrentDateHour(date);
                        String filePath = getFilePath(date);

                        bufferedWriter = HDFSUtils.getWriter(filePath);
                        LOGGER.info("hdfs file path: {}", filePath);
                    }

                    ConsumerRecords<String, String> records = consumer.poll(1000);
                    for (ConsumerRecord<String, String> record : records) {
                        bufferedWriter.append(record.value() + "\n");
//                        System.out.print(record.value() + "\n");
                    }
                }
            }catch (IOException e) {
                LOGGER.error("{} write hdfs error \n{}", threadName, ExceptionUtils.getFullStackTrace(e));
            }finally {
                try{
                    consumer.close();
                }catch (InterruptException e){
                    LOGGER.error("{} close KafkaConsumer error \n{}", threadName, ExceptionUtils.getFullStackTrace(e));
                }

                try {
                    bufferedWriter.close();
                } catch (IOException e) {
                    LOGGER.error("{} close BufferedWriter error \n{}", threadName, ExceptionUtils.getFullStackTrace(e));
                }
                LOGGER.info("{} has stopped", threadName);
            }
        }
    }


    public static void main(String[] args) {
        int threadNumber = 3;
        String checkFilePath = null;
        if(args.length == 1){
            threadNumber = Integer.valueOf(args[0]);
        }else if(args.length == 2){
            threadNumber = Integer.valueOf(args[0]);
            checkFilePath = args[1];
            LOGGER.info("thread number: {}, checkFile path: {}", threadNumber, checkFilePath);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);
        for (int i = 0; i < threadNumber; i++) {
            executorService.execute(new KafkaConsumerTask(checkFilePath));
        }

        executorService.shutdown();
    }
}