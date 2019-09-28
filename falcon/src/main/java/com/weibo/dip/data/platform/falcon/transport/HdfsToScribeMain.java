package com.weibo.dip.data.platform.falcon.transport;

import com.google.common.base.Stopwatch;
import com.weibo.dip.data.platform.QuartzScheduler;
import com.weibo.dip.data.platform.commons.util.DatetimeUtil;
import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import com.weibo.dip.data.platform.commons.util.RawlogUtil;
import com.weibo.dip.data.platform.falcon.transport.scribe.LogEntry;
import com.weibo.dip.data.platform.falcon.transport.scribe.Scribe;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hdfs to scribe.
 *
 * @author yurun
 */
public class HdfsToScribeMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsToScribeMain.class);

  private static final String CATEGORY = "category";
  private static final String HOST = "host";
  private static final String PORT = "port";

  public static class Pusher implements Runnable {
    private String category;
    private String file;
    private String host;
    private int port;

    /**
     * Construct a instance.
     *
     * @param category category
     * @param file file
     * @param host host
     * @param port port
     */
    public Pusher(String category, String file, String host, int port) {
      this.category = category;
      this.file = file;
      this.host = host;
      this.port = port;
    }

    @Override
    public void run() {
      TTransport transport = null;
      BufferedReader reader = null;

      try {
        transport = new TFramedTransport(new TSocket(new Socket(host, port)));
        Scribe.Client client = new Scribe.Client(new TBinaryProtocol(transport, false, false));

        reader =
            new BufferedReader(
                new InputStreamReader(HDFSUtil.openInputStream(file), CharEncoding.UTF_8));

        String line;
        while ((line = reader.readLine()) != null) {
          line += "\n";

          LogEntry entry = new LogEntry();

          entry.setCategory(category);
          entry.setMessage(ByteBuffer.wrap(line.getBytes(CharEncoding.UTF_8)));

          client.Log(Collections.singletonList(entry));
        }
      } catch (Exception e) {
        LOGGER.error("pusher run error: {}", ExceptionUtils.getFullStackTrace(e));
      } finally {
        if (Objects.nonNull(reader)) {
          try {
            reader.close();
          } catch (IOException e) {
            LOGGER.info("close reader for {} error: {}", ExceptionUtils.getFullStackTrace(e));
          }
        }

        if (Objects.nonNull(transport)) {
          transport.close();
        }
      }
    }
  }

  public static class PushJob implements Job {
    private void push(String catetory, List<String> files, String host, int port) {
      ExecutorService executor = Executors.newFixedThreadPool(3);

      for (String file : files) {
        executor.execute(new Pusher(catetory, file, host, port));
      }

      executor.shutdown();
      while (!executor.isTerminated()) {
        try {
          executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          // do nothing
        }
      }
    }

    @Override
    public void execute(JobExecutionContext context) {
      Date scheduledTime = context.getScheduledFireTime();
      Date fireTime = context.getFireTime();

      String timestamp = DatetimeUtil.DATETIME_FORMAT.format(scheduledTime);

      LOGGER.info(
          "timestamp: {}, delay: {} s",
          timestamp,
          (fireTime.getTime() - scheduledTime.getTime()) / 1000);

      JobDataMap jobDataMap = context.getMergedJobDataMap();

      String category = jobDataMap.getString(CATEGORY);
      String host = jobDataMap.getString(HOST);
      int port = jobDataMap.getIntValue(PORT);

      Date previousFireTime = context.getPreviousFireTime();
      if (Objects.isNull(previousFireTime)) {
        return;
      }

      List<String> files;

      try {
        files = RawlogUtil.getRangeTimeFiles(category, previousFireTime, scheduledTime);
        LOGGER.info(
            "timestamp: {}, files: {}",
            timestamp,
            CollectionUtils.isNotEmpty(files) ? files.size() : 0);
      } catch (Exception e) {
        LOGGER.error("get range time files error: {}", ExceptionUtils.getFullStackTrace(e));

        return;
      }

      if (CollectionUtils.isNotEmpty(files)) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        push(category, files, host, port);
        stopwatch.stop();

        LOGGER.info(
            "timestamp: {}, files: {}, push time: {} s",
            timestamp,
            files.size(),
            stopwatch.elapsed(TimeUnit.SECONDS));
      }
    }
  }

  /**
   * Main.
   *
   * @param args [category, scribe host, scribe port]
   */
  public static void main(String[] args) {
    try {
      String category = args[0];
      LOGGER.info("category: {}", category);

      String host = args[1];
      LOGGER.info("host: {}", host);

      int port = Integer.valueOf(args[2]);
      LOGGER.info("port: {}", port);

      QuartzScheduler scheduler = new QuartzScheduler();
      scheduler.start();

      JobDataMap jobDataMap = new JobDataMap();

      jobDataMap.put(CATEGORY, category);
      jobDataMap.put(HOST, host);
      jobDataMap.put(PORT, port);

      scheduler.schedule("FileJob", "0 0/3 * * * ?", PushJob.class, jobDataMap);

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      scheduler.shutdown();
                    } catch (SchedulerException e) {
                      LOGGER.error(
                          "scheduler shutdown error: {}", ExceptionUtils.getFullStackTrace(e));
                    }
                  }));
    } catch (Exception e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    }
  }
}
