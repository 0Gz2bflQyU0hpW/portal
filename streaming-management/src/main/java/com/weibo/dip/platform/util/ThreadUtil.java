package com.weibo.dip.platform.util;

import com.weibo.dip.platform.model.Streaming;
import com.weibo.dip.platform.service.StreamingService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadUtil {

  private static ExecutorService threadPool = Executors.newCachedThreadPool();
  private static StreamingService service = new StreamingService();

  /**
   * start application.
   *
   * @param streaming info
   */
  public static void startApplication(Streaming streaming) {
    threadPool.execute(
        new Runnable() {
          @Override
          public void run() {
            service.start(streaming);
          }
        });
  }

  /**
   * kill application.
   *
   * @param streaming info
   */
  public static void killApplication(Streaming streaming) {
    threadPool.execute(
        new Runnable() {
          @Override
          public void run() {
            service.kill(streaming);
          }
        });
  }

  /**
   * restart application.
   *
   * @param streaming info
   */
  public static void restartApplication(Streaming streaming) {
    threadPool.execute(
        new Runnable() {
          @Override
          public void run() {
            service.restart(streaming);
          }
        });
  }
}
