package com.weibo.dip.data.platform.commons.util;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by yurun on 16/12/21.
 */
public class HadoopConfiguration {

    private static final Configuration CONFIGURATION = new Configuration();

    public static Configuration getInstance() {
        return CONFIGURATION;
    }

}
