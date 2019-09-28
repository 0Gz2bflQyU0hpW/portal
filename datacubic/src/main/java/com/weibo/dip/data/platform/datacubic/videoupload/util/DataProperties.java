package com.weibo.dip.data.platform.datacubic.videoupload.util;

/**
 * Created by qianqian25 on 2017/11/29.
 */

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class DataProperties {

    private static final String STREAMING_CONFIG = "streaming.config";

    public static Map<String,String> loads()  throws IOException {
        Properties prop = new Properties();
        Map<String, String> map =  new HashMap<>();
        InputStream in = null;

        try{
            in = DataProperties.class.getClassLoader().getResourceAsStream(STREAMING_CONFIG);
            prop.load(in);
            for (String key : prop.stringPropertyNames()) {
                map.put(key, prop.getProperty(key));
            }
        } finally{
            if (Objects.nonNull(in)) {
                in.close();
            }
        }

        return map;
    }

}
