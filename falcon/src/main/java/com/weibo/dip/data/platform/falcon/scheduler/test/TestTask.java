package com.weibo.dip.data.platform.falcon.scheduler.test;

import com.weibo.dip.data.platform.falcon.scheduler.model.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Wen on 2017/2/17.
 */
public class TestTask extends Task {


    private static final Logger LOGGER = LoggerFactory.getLogger(TestTask.class);

    @Override
    public void setup() throws Exception {
        LOGGER.info(this.getJobName()+" has been set up");
    }

    @Override
    public void execute() throws Exception {
        LOGGER.info(this.getJobName() + " is executing");
        Thread.sleep(1000);
    }

    @Override
    public void cleanup() throws Exception {
        LOGGER.info(this.getJobName() + " is finished");
    }

    @Override
    public int compareTo(Task o) {
        return 0;
    }
}
