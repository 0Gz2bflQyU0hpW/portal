package com.weibo.dip.scheduler.test;

import com.weibo.dip.scheduler.bean.Application;
import com.weibo.dip.scheduler.client.SchedulerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author yurun */
public class VideoUserActionsDistributionMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(VideoUserActionsDistributionMain.class);

  private static final String HOST = "10.210.77.15";
  private static final int PORT = 8889;

  public static void start() throws Exception {
    SchedulerClient client = new SchedulerClient(HOST, PORT);

    Application application = new Application();

    application.setName("video_user_actions_distribution");
    application.setQueue("default");
    application.setUser("yurun");
    application.setPriority(0);
    application.setCores(1);
    application.setMems(2048);
    application.setRepository("registry.api.weibo.com/dippub/appengine");
    application.setTag("0.0.1");
    application.setCron("0 25 0/1 * * ?");
    application.setTimeout(3600);

    client.start(application);

    LOGGER.info("started");
  }

  public static void stop() throws Exception {
    SchedulerClient client = new SchedulerClient(HOST, PORT);

    client.stop("video_user_actions_distribution", "default");

    LOGGER.info("stoped");
  }

  public static void main(String[] args) throws Exception {
    String action = args[0];

    if (action.equals("start")) {
      start();
    } else if (action.equals("stop")) {
      stop();
    }
  }
}
