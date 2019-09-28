package com.weibo.dip.platform.zookeeper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.weibo.dip.platform.hessian.ClientServiceImpl;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by haisen on 2018/7/30. */
public class ProgramControl {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProgramControl.class);

  private static volatile boolean running = false;
  // 启动jar包 命令 需要路径参数，待确定。。。。。
  private static final String startcommand = "java -cp ";

  private static Runtime runtime = Runtime.getRuntime();
  private static volatile Process process = null;

  /** start jar. */
  public static void start() {
    if (!running) {
      System.out.println("Start");
      running = true;
      // 测试使用，线上删除
    }

    /*if (!running & process == null) {
      try {
        process = runtime.exec(startcommand);
        running = true;
        LOGGER.info("Exe start success , Command :{}", startcommand);
      } catch (IOException e) {
        LOGGER.error(ExceptionUtils.getFullStackTrace(e));
      }
    }*/
  }

  /** stop jar. */
  public static void stop() {
    if (running) {
      System.out.println("Stop");
      running = false;
    }
    /*if (running & process != null) {
      process.destroy();
      // todo : delete dockers
      LOGGER.info("destroy the process");
      try {
        process.waitFor(5000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOGGER.error(ExceptionUtils.getFullStackTrace(e));
      }
      process = null;
      running = false;
    }*/
  }
}
