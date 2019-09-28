package com.weibo.dip.warehouse;

import java.text.SimpleDateFormat;

/** @author yurun */
public class TimeFormat {
  public static void main(String[] args) throws Exception {
    String timeStr = "30/Aug/2018:15:25:31 +0800";

    SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");

    SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    System.out.println(format2.format(format.parse(timeStr)));
  }
}
