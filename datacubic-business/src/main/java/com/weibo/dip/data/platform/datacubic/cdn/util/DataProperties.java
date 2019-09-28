package com.weibo.dip.data.platform.datacubic.cdn.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class DataProperties {

  /**1.0, May 9, 2014
   * load the properties
   */

  public static Map<String, String> loads(String dir) throws IOException {
    Properties prop = new Properties();
    Map<String, String> map = new HashMap<>();
    InputStream in = null;

    try {
      in = DataProperties.class.getClassLoader().getResourceAsStream(dir);
      prop.load(in);
      for (String key : prop.stringPropertyNames()) {
        map.put(key, prop.getProperty(key));
      }
    } finally {
      if (Objects.nonNull(in)) {
        in.close();
      }
    }

    return map;
  }

}
