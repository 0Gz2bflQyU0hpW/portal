package com.weibo.dip.data.platform.falcon.transport.task;

import com.weibo.dip.data.platform.falcon.transport.utils.ConfigUtils;
import com.weibo.dip.data.platform.falcon.transport.utils.DateUtils;
import com.weibo.dip.data.platform.falcon.transport.utils.ProducerTaskUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static com.weibo.dip.data.platform.falcon.transport.utils.ConfigUtils.PRODUCER_CATEGORY_NAME;

/**
 * Created by Wen on 2017/3/7.
 */
public class AdditionProducer extends Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdditionProducer.class);

    private long begin = ConfigUtils.PRODUCER_ADDITION_START;

    private long endTime = ConfigUtils.PRODUCER_ADDITION_END;

    private boolean stopFlag = false;

    private Consumer consumer;

    public AdditionProducer(Consumer consumer) {
        this.consumer = consumer;
    }

    private class AdditionTask implements Runnable {

        @Override
        public void run() {
            StoreQueue queue = StoreQueue.getInstance();

            List<String> pathList = null;

            while (!stopFlag && begin < endTime) {
                if (queue.isEmpty()) {

                    long end = begin + ConfigUtils.PRODUCER_ADDITION_INTERVAL;

                    if (end >= endTime) {
                        end = endTime;
                    }

                    try {
                        pathList = ProducerTaskUtils.getRecentFinishedLogByURL(PRODUCER_CATEGORY_NAME, begin, end);
                        LOGGER.info("Get " + PRODUCER_CATEGORY_NAME + " success, the Time from " + DateUtils.long2String(begin) + " to " + DateUtils.long2String(end) + ".");
                    } catch (InvalidKeyException | NoSuchAlgorithmException | IOException e) {
                        LOGGER.error("Get " + PRODUCER_CATEGORY_NAME + " failed, the Time from " + DateUtils.long2String(begin) + "to" + DateUtils.long2String(end) + "." + ExceptionUtils.getFullStackTrace(e));
                    }

                    if (CollectionUtils.isNotEmpty(pathList)) {
                        queue.offer(pathList);
                    }

                    begin = end + 1000;  //请求为闭区间，所以要在之前基础上+1s
                } else {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        LOGGER.info("Addition thread sleep interrupted");
                    }

                }
            }

            if (stopFlag) {
                LOGGER.warn("Addition Task stopped.Closed by CloseThread");
                return;
            }

            try {
                stop();
            } catch (Exception e) {
                LOGGER.error("Producer stop error: " + ExceptionUtils.getFullStackTrace(e));
            }

            try {
                consumer.stop();
            } catch (Exception e) {
                LOGGER.error("Consumer stop error: " + ExceptionUtils.getFullStackTrace(e));
            }

            LOGGER.info("Addition Task stopped");
        }

    }

    @Override
    public void start() throws Exception {
        new Thread(new AdditionTask()).start();
    }

    @Override
    public void stop() throws Exception {
        stopFlag = true;
    }

}
