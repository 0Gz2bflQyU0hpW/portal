package com.weibo.dip.data.platform.falcon.transport.task;

import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import com.weibo.dip.data.platform.falcon.transport.utils.ConfigUtils;
import com.weibo.dip.data.platform.falcon.transport.utils.KafkaSender;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Created by Wen on 2017/2/19.
 */
public class Consumer {

    private final static Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private Worker worker = new Worker();

    private KafkaSender sender;

    private class HdfsFileReader implements Iterator {

        private String filePath;

        private BufferedReader bufferedReader;

        private String line;

        HdfsFileReader(String filePath) throws IOException {
            this.filePath = filePath;

            bufferedReader = new BufferedReader(new InputStreamReader(HDFSUtil.openInputStream(filePath), CharEncoding.UTF_8));
        }

        @Override
        public boolean hasNext() {
            try {
                line = bufferedReader.readLine();
            } catch (IOException e) {
                line = null;

                LOGGER.error("FilePath " + filePath + " read error: " + ExceptionUtils.getFullStackTrace(e));
            }

            return Objects.nonNull(line);
        }

        @Override
        public String next() {
            return line;
        }

        public void close() {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    LOGGER.error("FilePath" + filePath + " close error: " + ExceptionUtils.getFullStackTrace(e));
                }
            }
        }
    }

    private class Worker {

        private boolean stopFlag = false;

        private ThreadPoolExecutor pool = new ThreadPoolExecutor(ConfigUtils.CONSUMER_THREAD_NUM, ConfigUtils.CONSUMER_THREAD_NUM, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

        Worker() {

        }

        public void start() {
            for (int index = 0; index < ConfigUtils.CONSUMER_THREAD_NUM; index++) {
                pool.execute(new ConsumeJob());
            }
        }

        void stop() throws InterruptedException {
            stopFlag = true;
            pool.shutdown();

            while (!pool.isTerminated()) {
                LOGGER.info("Consumer active works: " + pool.getActiveCount() + " And still " + StoreQueue.getInstance().size() + " files in waittingQueue.");

                Thread.sleep(1000);
            }
        }

        private class ConsumeJob implements Runnable {

            private boolean matchPath(String path) {
                return Pattern.matches("/user/hdfs/rawlog/\\w+/\\d{4}_\\d{2}_\\d{2}/\\d{2}/\\S+", path);
            }

            @Override
            public void run() {
                while (!stopFlag || !StoreQueue.getInstance().isEmpty()) {
                    String filePath;

                    try {
                        filePath = StoreQueue.getInstance().poll(ConfigUtils.PRODUCER_GET_INTERVAL, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        LOGGER.info("ConsumeJob " + Thread.currentThread().getName() + " interrupted");

                        return;
                    }

                    if (StringUtils.isEmpty(filePath)) {
                        continue;
                    }

                    if (!matchPath(filePath)) {
                        LOGGER.error("Invalid hdfs file path: " + filePath);

                        continue;
                    }

                    try {
                        if (!HDFSUtil.exist(filePath)) {
                            LOGGER.warn("HDFS file path: " + filePath + " not exist");

                            continue;
                        }
                    } catch (IOException e) {
                        LOGGER.error("HDFS check filePath :" + filePath + " failed: " + ExceptionUtils.getFullStackTrace(e));

                        return;
                    }

                    HdfsFileReader hdfsFileReader = null;

                    try {
                        hdfsFileReader = new HdfsFileReader(filePath);

                        long begin = System.currentTimeMillis();

                        String content;

                        LOGGER.info("Start to send " + filePath + " to kafka");

                        while (hdfsFileReader.hasNext()) {
                            content = hdfsFileReader.next();

                            if (StringUtils.isNotEmpty(content)) {
                                sender.sendMessgage(content);
                            }
                        }

                        long end = System.currentTimeMillis();

                        LOGGER.info("Send filePath " + filePath + " success, fileSize: " + HDFSUtil.summary(filePath).getLength() + ", consumeTime: " + (end - begin) + " ms");

                    } catch (Exception e) {
                        LOGGER.error("send " + filePath + " error: " + ExceptionUtils.getFullStackTrace(e));
                    } finally {
                        if (hdfsFileReader != null) {
                            hdfsFileReader.close();
                        }
                    }
                }
            }
        }

    }

    public void start() throws Exception {
        sender = KafkaSender.getInstance();

        worker.start();
    }

    public void stop() throws Exception {
        worker.stop();

        sender.close();
    }
}
