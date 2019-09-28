package com.weibo.dip.cdn.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class DataProperties {


    public static Map<String,String> loads(String dir)  throws IOException {
        Properties prop = new Properties();
        Map<String, String> map =  new HashMap<>();
        InputStream in = null;

        try{
            in = DataProperties.class.getClassLoader().getResourceAsStream(dir);
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

    public static void main(String[] msg) throws Exception {
        final String STREAMING_CONFIG = "videoupload.properties";
        Map<String, String> configMsg = DataProperties.loads(STREAMING_CONFIG);
        Map<String, Object> producerConfig = GsonUtil.fromJson(configMsg.get("a"), GsonUtil.GsonType.OBJECT_MAP_TYPE);
        System.out.println(producerConfig);
    }

}
