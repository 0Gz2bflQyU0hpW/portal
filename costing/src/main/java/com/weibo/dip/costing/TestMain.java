package com.weibo.dip.costing;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by yurun on 18/4/24.
 */
public class TestMain {

  public static void main(String[] args) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    Calendar now = Calendar.getInstance();

    now.set(Calendar.YEAR, 2017);
    now.set(Calendar.MONTH, 0);
    now.set(Calendar.DAY_OF_MONTH,now.getActualMaximum(Calendar.DAY_OF_MONTH));

    System.out.println(sdf.format(now.getTime()));
  }

}
