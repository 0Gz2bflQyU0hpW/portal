package com.weibo.dip.scheduler.test;

import com.weibo.dip.scheduler.client.SchedulerClient;

/** @author yurun */
public class StopTest2Main {
  public static void stop() throws Exception {
    String host = "localhost";
    int port = 8889;

    SchedulerClient client = new SchedulerClient(host, port);

    client.stop("test2", "default");
  }

  public static void main(String[] args) throws Exception {
    stop();
  }
}
