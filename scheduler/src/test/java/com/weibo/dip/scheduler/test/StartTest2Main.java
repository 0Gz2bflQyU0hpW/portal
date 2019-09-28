package com.weibo.dip.scheduler.test;

import com.weibo.dip.scheduler.bean.Application;
import com.weibo.dip.scheduler.client.SchedulerClient;

/** @author yurun */
public class StartTest2Main {
  public static void start() throws Exception {
    String host = "localhost";
    int port = 8889;

    SchedulerClient client = new SchedulerClient(host, port);

    Application application = new Application();

    application.setName("test2");
    application.setQueue("default");
    application.setUser("yurun");
    application.setPriority(0);
    application.setCores(1);
    application.setMems(1);
    application.setRepository("");
    application.setTag("");
    application.setCron("0/20 * * * * ? *");

    client.start(application);
  }

  public static void main(String[] args) throws Exception {
    start();
  }
}
