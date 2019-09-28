package com.weibo.dip.portal.util;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * @brief 配置文件取值类
 * @author liunan
 * @date 2016/11/7
 */
public class PropertyUtil implements ApplicationContextAware {
    private static ApplicationContext applicationContext;

    /**
     * 获取property文件中key对应的值
     *
     * @param key
     * @return String
     */
    public static String getString(String key) {
        return applicationContext.getMessage(key, null, null);
    }

    /**
     * 当取不到值时，返回传入的默认值
     *
     * @param key
     * @param defaultValue
     * @return String
     */
    public static String getString(String key, String defaultValue) {
        if (StringUtils.isEmpty(getString(key))) {
            return defaultValue;
        }
        return getString(key);
    }

    /**
     * 获取property文件中key对应的值
     *
     * @param key
     * @return double
     */
    public static double getDouble(String key) {
        return Double.parseDouble(getString(key));
    }

    /**
     * 获取property文件中key对应的值
     *
     * @param key
     * @return int
     */
    public static int getInt(String key) {
        return Integer.parseInt(getString(key));
    }

    /**
     * 获取property文件中key对应的值
     *
     * @param key
     * @return int
     */
    public static int getInt(String key, int defaultValue) {
        if (StringUtils.isEmpty(getString(key))) {
            return defaultValue;
        }
        return getInt(key);
    }
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        PropertyUtil.applicationContext = applicationContext;
    }
}
