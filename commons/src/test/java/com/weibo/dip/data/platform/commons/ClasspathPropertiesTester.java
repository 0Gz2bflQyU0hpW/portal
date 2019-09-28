package com.weibo.dip.data.platform.commons;

/** @author yurun */
public class ClasspathPropertiesTester {
  public static void main(String[] args) throws Exception {
    ClasspathProperties properties = new ClasspathProperties(args[0]);

    System.out.println(properties.getString("key1"));
    System.out.println(properties.getFloat("key3"));
    System.out.println(properties.getDouble("key4"));
  }
}
