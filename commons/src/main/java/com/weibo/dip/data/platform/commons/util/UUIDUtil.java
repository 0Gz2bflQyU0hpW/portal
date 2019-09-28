package com.weibo.dip.data.platform.commons.util;

import java.util.UUID;

/**
 * uuid util.
 *
 * @author yurun
 */
public class UUIDUtil {

  public static String getUUID() {
    return UUID.randomUUID().toString();
  }

  public static String getDIPUID() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  public static void main(String[] args) {
    for (int i = 0; i < 100000; i++) {
      System.out.println(getDIPUID().length());
    }
  }
}
