package com.weibo.dip.data.platform.commons;

/** @author yurun */
public class DynamicClasspathPropertiesTester {
  public static void main(String[] args) throws Exception {
    ClasspathProperties properties = new DynamicClasspathProperties(args[0], 3000);

    int count = 0;

    while (++count < 100) {
      System.out.println(properties.getString("key1"));
      System.out.println(properties.getFloat("key3"));
      System.out.println(properties.getDouble("key4"));

      Thread.sleep(5 * 1000);
    }
  }
}
