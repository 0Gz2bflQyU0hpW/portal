package com.weibo.dip.data.platform.datacubic.util;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author delia
 */

public class LoggerUtil {

    private static final String DATACUBIC_LOG_NAME = "datacubic.log.name";

    public static Logger getLogger(Class clazz, String logName) {
        System.setProperty(DATACUBIC_LOG_NAME, logName);
        PropertyConfigurator.configure(LoggerUtil.class.getClassLoader().getResourceAsStream("datacubic_log4j.properties"));
        return LoggerFactory.getLogger(clazz);
    }

    public static Logger getLogger(Class clazz) {
        return getLogger(clazz, clazz.getSimpleName());
    }

}
