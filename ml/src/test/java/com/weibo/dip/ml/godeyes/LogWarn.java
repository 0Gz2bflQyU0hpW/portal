package com.weibo.dip.ml.godeyes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 17/7/13.
 */
public class LogWarn {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogWarn.class);

    public static void main(String[] args) {
        LOGGER.warn("{}, {}, {}", "1", "2", "3");
        LOGGER.warn("[{}, {}, {}]", "1", "2", "3");
    }

}
