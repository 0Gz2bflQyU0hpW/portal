package com.weibo.dip.data.platform.falcon.transport.task;

import com.weibo.dip.data.platform.falcon.transport.utils.ConfigUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;

import static com.weibo.dip.data.platform.falcon.transport.utils.ConfigUtils.PRODUCER_CATEGORY_NAME;
import static com.weibo.dip.data.platform.falcon.transport.utils.ProducerTaskUtils.getRecentFinishedLogByURL;

/**
 * Created by Wen on 2017/2/22.
 */
public class ProducerTask implements Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerTask.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        long end = context.getScheduledFireTime().getTime();
        long begin = end - ConfigUtils.PRODUCER_GET_INTERVAL + 1000;

        List<String> fileList;

        try {
            fileList = getRecentFinishedLogByURL(PRODUCER_CATEGORY_NAME, begin, end);
        } catch (InvalidKeyException | NoSuchAlgorithmException | IOException e) {
            LOGGER.error("Get " + PRODUCER_CATEGORY_NAME + "failed, the Time from " + begin + "to" + end + "." + ExceptionUtils.getFullStackTrace(e));

            return;
        }

        if (CollectionUtils.isNotEmpty(fileList)) {
            StoreQueue.getInstance().offer(fileList);
        }

        LOGGER.info(PRODUCER_CATEGORY_NAME + " get recent files: " + (Objects.isNull(fileList) ? 0 : fileList.size()));
    }

}
