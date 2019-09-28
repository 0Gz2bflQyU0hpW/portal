package com.weibo.dip.data.platform.datacubic.youku.test;

import com.weibo.dip.data.platform.datacubic.util.LoggerUtil;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Created by delia on 2017/1/4.
 */
public class TestLog {
    private static Logger LOGGER1= LoggerUtil.getLogger(TestLog.class,"testLog1");
    private static Logger LOGGER2= LoggerUtil.getLogger(TestLog.class,"testLog2");

    public static void main(String[] args) {
        while (true){
            LOGGER1.info("A\n");
            LOGGER2.info("B\n");
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                System.exit(0);
            }
        }
    }
}
