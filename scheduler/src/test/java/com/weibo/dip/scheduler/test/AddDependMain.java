package com.weibo.dip.scheduler.test;

import com.weibo.dip.scheduler.bean.ApplicationDependency;
import com.weibo.dip.scheduler.client.SchedulerClient;

/** @author yurun */
public class AddDependMain {
  public static void main(String[] args) throws Exception {
    String host = "localhost";
    int port = 8889;

    SchedulerClient client = new SchedulerClient(host, port);

    ApplicationDependency dependency = new ApplicationDependency();

    dependency.setName("test2");
    dependency.setQueue("default");
    dependency.setDependName("test");
    dependency.setDependQueue("default");
    dependency.setFromSeconds(30);
    dependency.setToSeconds(0);

    client.addDependency(dependency);
  }
}
