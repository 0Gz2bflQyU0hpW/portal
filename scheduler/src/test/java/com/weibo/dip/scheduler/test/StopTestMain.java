package com.weibo.dip.scheduler.test;

import com.weibo.dip.scheduler.client.SchedulerClient;

/** @author yurun */
public class StopTestMain {
  public static void stop() throws Exception {
    String host = "10.210.77.15";
    int port = 8889;

    SchedulerClient client = new SchedulerClient(host, port);

    client.stop("video_plays_distribution", "default");
  }

  public static void main(String[] args) throws Exception {
    stop();
  }
}
